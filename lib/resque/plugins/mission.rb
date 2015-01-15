require 'resque'
require 'resque-status'

module Resque
  module Plugins
    class Mission

      attr_reader :progress

      # When subclassing Task, we also want to make it available as a resque job
      def self.inherited(subklass)
        subklass.const_set('Job', Class.new(Mission::Job))
        subklass::Job.const_set('TASK_CLASS', subklass)
      end

      # Internal: The list of steps to be run on #call
      def self.steps
        @steps ||= []
      end

      # Public: Override this in your class to allow new tasks to be created from
      # the queue. The given args will be what is fed to .queue
      #
      # args - from self.queue!
      def self.create_from_options(args={})
        new args
      end

      # Public: Create a Queue Job.
      #
      # args - Hash that will be fed to self.create_from_options from the queue.
      def self.queue!(args={})
        self::Job.create({'args' => args})
      end

      # Public: Sets the queue name for resque
      def self.queue(q=nil)
        if q then @queue = q.to_s
        else @queue ||= "statused"
        end
      end

      # Public: Declare a step to be run on #call. Steps will be run in the order
      # they are declared.
      #
      # method_name - a symbol name for the method to be called.
      # options - An optional Hash of information about the step.
      #     current arguments:
      #     - :message => Used for status updates, vs. method_name titlecased
      def self.step(method_name, options={})
        steps << [method_name, options]
      end

      # Public: Perform the Mission
      #
      # status - Optional Mission::Progress object
      # block - if given, will yield idx, total, status message to the block
      #         before performing the step
      def call(status=nil, callbacks=nil)
        @progress = status || Progress.new
        start_timer
        steps = @steps
        if(steps.nil? || steps.size <= 0)
          steps = self.class.steps
        end
        steps.each_with_index do |step, index|
          method_name, options = step
          next if progress.completed?(method_name.to_s)
          progress.start method_name.to_s

          begin
            #name = options[:message] || (method_name.to_s.gsub(/\w+/) {|word| word.capitalize})
            name = (method_name.to_s.gsub(/\w+/) {|word| word.capitalize})

            Rails.logger.info ""
            Rails.logger.info ""
            Rails.logger.info "start step #{index}: #{name}"

            if(callbacks && callbacks[:at])
              callbacks[:at].call(index, steps.length, name, progress:progress)
            end
            send 'set_status_procs', callbacks[:get_status], callbacks[:set_status]
            send method_name
          rescue => e
            #if exception, need to clear working status and push this to redis
            progress.stop_working
            if(callbacks && callbacks[:at])
              callbacks[:at].call(index, steps.length, name, progress:progress)
            end
            raise e
          end

        end
        progress.finish
        if(callbacks && callbacks[:at])
          callbacks[:at].call(steps.length, steps.length, 'all-done', progress:progress)
        end
      rescue Object => e
        progress.failures += 1
        raise e
      end

      private
      # Private: start the timer
      def start_timer
        @start_time = Time.now.to_f
      end

      # Private: time since #call was called and now
      def delta_time
        return 0 unless @start_time
        Time.now.to_f - @start_time
      end

      # Private: Key for Statsd
      def stats_key(key=nil)
        @key_base ||= self.class.name.downcase.gsub(/\s+/,"_").gsub('::','.')
        key ? "#{@key_base}.#{key}" : @key_base
      end

      public

      class Job < Resque::JobWithStatus
        # Internal: used by Resque::JobWithStatus to get the queue name
        def self.queue
          self::TASK_CLASS.queue
        end

        #add this method because resque-scheduler needs a four-param method

        # Wrapper API to forward a Resque::Job creation API call into a Resque::Plugins::Status call.
        # This is needed to be used with resque scheduler
        # http://github.com/bvandenbos/resque-scheduler
        def self.scheduled(queue, klass, *args)
          Rails.logger.info ""
          Rails.logger.info ""
          Rails.logger.info "This is a retry job: #{klass}, #{args[0]}"
          Resque.enqueue_to(queue, klass, *args)
          uuid = args[0]
        end

        def safe_perform!
          #now get job progress from status key.
          @last_progress = Progress[{}]
          if(status && status['progress'])
            @last_progress = Progress[status['progress']]
          end
          super
        end

        # Internal: called by Resque::JobWithStatus to perform the job
        def perform
          Rails.logger.info ""
          Rails.logger.info ""
          Rails.logger.info "start perform job: #{@options['args']}, #{@last_progress}"

          task = self.class::TASK_CLASS.create_from_options(@options['args'])
          @options['progress'] = @last_progress
          #@options['progress'] = Progress[@options['progress'] || {}]

          callbacks = {}
          callbacks[:at] = Proc.new do |idx,total,*msg|
            at idx, total, *msg
          end
          callbacks[:set_status] = Proc.new do |key, value|
            self.status = [status, {key  => value}].flatten
          end
          callbacks[:get_status] = Proc.new do |key|
            status[key]
          end
          task.call(@options['progress'], callbacks)
          completed

          Rails.logger.info "end perform job: "
          Rails.logger.info ""
          Rails.logger.info ""
        rescue => e
          Rails.logger.error "resque-mission:perform error: #{e} callstack:#{e.backtrace}"
          raise e
        end

        # Internal: used by Resque::JobWithStatus to handle failure
        # Stores the progress object on the exception so we can pass it through
        # to the resque callback and store it in the failure.
        def on_failure(e)
          #e.instance_variable_set :@job_progress, @options['progress']
          raise e
        end

        # Internal: resque on failure callback, sorted
        # Takes our progress object and injects it back into the arguments hash
        # so that when the job is retried it knows where to resume.
        def self.on_failure_1111_store_progress(e, *args)
          #set this will change job args, so this will be a different job in resque-retry, because it uses class+arg to gen the signiture
          #args.last['progress'] = e.instance_variable_get :@job_progress
          args.last.delete('progress')
        end
      end

      class Progress < Hash
        def failures
          self['failures'] ||= 0
        end

        def failures=(int)
          self['failures'] = int
        end

        def start(step)
          completed.push delete('working') if working
          self['working'] = step
        end

        def stop_working
          delete('working') if working
        end

        def working
          self['working']
        end

        def completed
          self['completed'] ||= []
        end

        def completed?(step)
          completed.include?(step)
        end

        def finish
          completed.push delete('working') if self['working']
          self['finished'] = true
        end

        def finished?
          self['finished'].present?
        end
      end
    end
  end
end