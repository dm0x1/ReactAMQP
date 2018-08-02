<?php

namespace Gos\Component\ReactAMQP;

use AMQPQueue;
use BadMethodCallException;
use Evenement\EventEmitter;
use React\EventLoop\LoopInterface;

/**
 * Class to listen to an AMQP queue and dispatch listeners when messages are
 * received.
 *
 * @author  Jeremy Cook <jeremycook0@gmail.com>
 */
class Consumer extends EventEmitter
{
	/**
	 * AMQP message queue to read messages from.
	 *
	 * @var AMQPQueue
	 */
	protected $queue;

	/**
	 * Event loop.
	 *
	 * @var \React\EventLoop\LoopInterface
	 */
	protected $loop;

	/**
	 * Flag to indicate if this listener is closed.
	 *
	 * @var bool
	 */
	protected $closed = false;

	/**
	 * Max number of messages to consume in a 'batch'. Should stop the event
	 * loop stopping on this class for protracted lengths of time.
	 *
	 * @var int
	 */
	protected $max;

	/**
	 * @var \React\EventLoop\Timer\TimerInterface
	 */
	private $timer;

	/**
	 * Set the number of messages to prefetch from the broker.
	 *
	 * @var int
	 */
	private $prefetchCount;

	/**
	 * @var int
	 */
	private $unprocessedCount;

	/**
	 * Constructor. Stores the message queue and the event loop for use.
	 *
	 * @param AMQPQueue                      $queue    Message queue
	 * @param \React\EventLoop\LoopInterface $loop     Event loop
	 * @param float                          $interval Interval to check for new messages
	 * @param int                            $max      Max number of messages to consume in one go
	 */
	public function __construct(AMQPQueue $queue, LoopInterface $loop, $interval, $max = null)
	{
		$this->queue         = $queue;
		$this->loop          = $loop;
		$this->max           = $max;
		$this->timer         = $this->loop->addPeriodicTimer($interval, $this);
		$this->prefetchCount = 0;

		$this->on('close_amqp_consumer', [$this, 'close']);
	}

	/**
	 * Please use Consumer->ack(), Consumer->nack(), Consumer->reject()
	 *
	 * @param $count
	 *
	 * @throws UnexpectedTypeException
	 */
	public function setPrefetchCount($count)
	{
		if (!is_int($count)) {
			throw new UnexpectedTypeException(sprintf('Incorrect `count` type: integer required, %s given', gettype($count)));
		}

		if ($count < 0) {
			throw new \RuntimeException('Count must greater than 0 or equal to 0');
		}

		$this->prefetchCount = $count;
	}

	/**
	 * @param \AMQPEnvelope $envelope
	 * @param int           $flags
	 *
	 * @return bool
	 * @throws \AMQPChannelException
	 * @throws \AMQPConnectionException
	 */
	public function ack(\AMQPEnvelope $envelope, $flags = AMQP_NOPARAM)
	{
		if ($this->queue->ack($envelope->getDeliveryTag(), $flags)) {
			$this->unprocessedCount --;
			return true;
		}

		return false;
	}

	/**
	 * @param \AMQPEnvelope $envelope
	 * @param int           $flags
	 *
	 * @return bool
	 * @throws \AMQPChannelException
	 * @throws \AMQPConnectionException
	 */
	public function nack(\AMQPEnvelope $envelope, $flags = AMQP_REQUEUE)
	{
		if ($this->queue->nack($envelope->getDeliveryTag(), $flags)) {
			$this->unprocessedCount --;
			return true;
		}

		return false;
	}

	/**
	 * @param \AMQPEnvelope $envelope
	 * @param int           $flags
	 *
	 * @return bool
	 * @throws \AMQPChannelException
	 * @throws \AMQPConnectionException
	 */
	public function reject(\AMQPEnvelope $envelope, $flags = AMQP_NOPARAM)
	{
		if ($this->queue->reject($envelope->getDeliveryTag(), $flags)) {
			$this->unprocessedCount --;
			return true;
		}

		return false;
	}

	/**
	 * Method to handle receiving an incoming message.
	 *
	 * @throws BadMethodCallException
	 */
	public function __invoke()
	{
		if ($this->closed) {
			throw new BadMethodCallException('This consumer object is closed and cannot receive any more messages.');
		}

		$counter = 0;
		while (($this->prefetchCount === 0 || ($this->prefetchCount > 0 && $this->unprocessedCount < $this->prefetchCount)) && $envelope = $this->queue->get()) {

			if ($this->prefetchCount > 0) {
				$this->unprocessedCount ++;
			}

			$this->emit('consume', [$envelope, $this->queue]);

			if ($this->prefetchCount > 0) {
				if ($this->unprocessedCount >= $this->prefetchCount) {
					return;
				}
			}

			if ($this->max && ++ $counter >= $this->max) {
				return;
			}
		}
	}

	/**
	 * Allows calls to unknown methods to be passed through to the queue
	 * stored.
	 *
	 * @param string $method Method name
	 * @param mixed  $args   Args to pass
	 *
	 * @return mixed
	 */
	public function __call($method, $args)
	{
		return call_user_func_array([$this->queue, $method], $args);
	}

	/**
	 * Method to call when stopping listening to messages.
	 */
	public function close()
	{
		if ($this->closed) {
			return;
		}

		$this->emit('end', [$this]);
		$this->loop->cancelTimer($this->timer);
		$this->removeAllListeners();
		unset($this->queue);
		$this->closed = true;
	}
}
