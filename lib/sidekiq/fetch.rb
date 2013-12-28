require 'sidekiq'
require 'sidekiq/actor'
require 'bunny'

module Sidekiq
  ##
  # The Fetcher blocks on Redis, waiting for a message to process
  # from the queues.  It gets the message and hands it to the Manager
  # to assign to a ready Processor.
  class Fetcher
    include Util
    include Actor

    TIMEOUT = 1
    SLEEP   = 0.01 #10ms

    def initialize(mgr, options)
      @down = nil
      @mgr = mgr
      @strategy = Fetcher.strategy.new(options)
    end

    # Fetching is straightforward: the Manager makes a fetch
    # request for each idle processor when Sidekiq starts and
    # then issues a new fetch request every time a Processor
    # finishes a message.
    #
    # Because we have to shut down cleanly, we can't block
    # forever and we can't loop forever.  Instead we reschedule
    # a new fetch if the current fetch turned up nothing.
    def fetch
      watchdog('Fetcher#fetch died') do
        return if Sidekiq::Fetcher.done?

        begin
          work = @strategy.retrieve_work
          ::Sidekiq.logger.info("Redis is online, #{Time.now.to_f - @down.to_f} sec downtime") if @down
          @down = nil

          if work
            @mgr.async.assign(work)
          else
            after(0) { fetch }
          end
        rescue => ex
          handle_fetch_exception(ex)
        end

      end
    end

    def handle_fetch_exception(ex)
      if !@down
        logger.error("Error fetching message: #{ex}")
        ex.backtrace.each do |bt|
          logger.error(bt)
        end
      end
      @down ||= Time.now
      sleep(TIMEOUT)
      after(0) { fetch }
    rescue Task::TerminatedError
      # If redis is down when we try to shut down, all the fetch backlog
      # raises these errors.  Haven't been able to figure out what I'm doing wrong.
    end

    # Ugh.  Say hello to a bloody hack.
    # Can't find a clean way to get the fetcher to just stop processing
    # its mailbox when shutdown starts.
    def self.done!
      @done = true
    end

    def self.done?
      @done
    end

    def self.strategy
      Sidekiq.options[:fetch] || BasicFetch
    end
  end

  class BasicFetch
    def initialize(options)
      @strictly_ordered_queues = !!options[:strict]
      #@queues = options[:queues].map{|q| "queue:#{q}"}
      @queues = options[:queues]
      @unique_queues = @queues.uniq
    end

    def retrieve_work
      time_waiting = 0.0
      queues = @strictly_ordered_queues ? @unique_queues.dup : @queues.shuffle.uniq
      
      # This is not really the optimal configuration
      Sidekiq.bunny do |channel|
        begin
          queues.each do |queue_name|
            q = channel.queue(Sidekiq::canonical_queue_name(queue_name), :durable => true)
            next if !q
            delivery_info, properties, work = q.pop({:ack => true})  
            return UnitOfWork.new(*[queue_name, work, properties, delivery_info]) if work
          end

          sleep(Sidekiq::Fetcher::SLEEP)
          time_waiting += Sidekiq::Fetcher::SLEEP
        end until time_waiting > Sidekiq::Fetcher::TIMEOUT
      end

    end

    # By leaving this as a class method, it can be pluggable and used by the Manager actor. Making it
    # an instance method will make it async to the Fetcher actor
    def self.bulk_requeue(inprogress, options)
      return if inprogress.empty?

      Sidekiq.logger.debug { "Re-queueing terminated jobs" }

      inprogress.each do |unit_of_work|
        unit_of_work.requeue
      end

      Sidekiq.logger.info("Pushed #{inprogress.size} messages back to Redis")
    rescue => ex
      Sidekiq.logger.warn("Failed to requeue #{inprogress.size} jobs: #{ex.message}")
    end

    UnitOfWork = Struct.new(:queue, :message, :properties, :delivery_info) do
      def acknowledge
#        Sidekiq.logger.debug { delivery_info }
        Sidekiq.logger.debug { properties }

        Sidekiq.bunny do |channel|
          channel.ack(delivery_info.delivery_tag, false)
        end
      end

      def queue_name
        queue.gsub(/.*queue:/, '')
      end

      def requeue
        Sidekiq.bunny do |channel|
          channel.reject(delivery_info.delivery_tag, true)
        end
      end
    end

    # Creating the Redis#blpop command takes into account any
    # configured queue weights. By default Redis#blpop returns
    # data from the first queue that has pending elements. We
    # recreate the queue command each time we invoke Redis#blpop
    # to honor weights and avoid queue starvation.
    def queues_cmd
      queues = @strictly_ordered_queues ? @unique_queues.dup : @queues.shuffle.uniq
      queues << Sidekiq::Fetcher::TIMEOUT
    end
  end
end
