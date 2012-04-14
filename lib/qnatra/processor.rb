require 'rubygems'
require 'bunny'
require 'thread'

#
# Baseclass to implement processors to process queue events (consumer role)
#
# define processing instructions in a sintra style like:
#    process :exchange => "public.mail.notifications", :queue => "mail.notification.processor1", :key=>"*" do |msg|
#       puts "received msg #{msg[:payload]}"
#    end
#
module Qnatra
  class Processor

    class << self

      # @group Setup of Handlers
      
      # You can add several error-handlers giving the error-method a block to execute with every error.
      # 
      # Here is a example how to use it.
      #  class ErrorLoggingProcessor < Qnatra::Processor
      #    error do |exception|
      #      # Do something with the error, for example:
      #      Logger.error exception.message
      #    end
      #  end
      def error(&block)
        @error_handler ||= []
        @error_handler << block
      end
  
      # For every successful handled Message, these handlers are called.
      #
      # Of course, it can be used for Logging:
      #   class SuccessLoggingProcessor < Qnatra::Processor
      #     success do |meta|
      #       # meta is a Hash containing :msg, :queue, :exchange, :topic and :duration
      #       Logger.info "Message %s/%s/%s in %dms" % [:queue, :exchange, :topic].map { |s| meta[s] }
      #     end
      #   end
      def success(&block)
        @success_handler ||= []
        @success_handler << block
      end
  
      # Handling of the three system events: startup, connect and error. On each, all system handlers are called with the event and some information-string.
      #
      # How about a Logging-Example:
      #   class SystemLoggingProcessor < Qnatra::Processor
      #     system_event do |event, message|
      #       # Possible events: :startup, :connect, :error
      #       Logger.info "System #{event}: #{message}
      #     end
      #   end
      def system_event(&block) 
        @sysevent_handler ||= []
        @sysevent_handler << block
      end

      # @endgroup
  
      # define a process
      def process(args, &block)
        @processes ||= [] 
        @processes << args.merge({:block => block })
      end
  
  
      def outbound(name, exchange, type)
        @outbounds ||= {}
        @outbounds[name] = {:exchange_name => exchange, :type => type }
      end
  
      def post(outbound_name, payload, opts = {})
        outbound = @outbounds[outbound_name]
        outbound[:exchange].publish(payload, opts) 
      end
  
      def ensync(&block)
        @sync_queue << block  
      end
  
  
      # start execution
      # 
      # for a single rabbitmq-host set 
      #   settings[:host] => hostname
      # for a cluster set
      #   settintgs[:hosts] => [host1, host2...]
      #
      def start(settings = Hash.new)
        # ensure all handler arrays are initialized
        @error_handler   ||= []
        @success_handler ||= []
        @processes       ||= []
        @sysevent_handler ||= []
        @sync_queue ||= Queue.new
        sys_event :startup, 'startup'
        @stopped = false
  
        pop_settings = settings.reject { |k,v| k != :ack }
  
        begin
          next_host(settings)
          
          sys_event :connect, "opening client on #{settings[:host]}:#{settings[:port]}"
          client = Bunny.new(settings)
          client.start 
  
          @outbounds ||= {}
          @outbounds.each do |k,v| 
            exchange = client.exchange(v[:exchange_name], :type => v[:type])
            @outbounds[k][:exchange] = exchange
          end
  
          @processes.each do |p| 
            exchange = client.exchange( p[:exchange], :type => :topic )
            queue = client.queue( p[:queue] )
            if p[:key].is_a? Array
              p[:key].each do | rk |
                queue.bind(exchange, :key => rk)
              end
            else
              queue.bind(exchange, :key => p[:key])
            end
            p[:the_queue] = queue
          end 
  
          # endless loop and pop queues
          while !@stopped 
            got_a_msg = false
            @processes.each do |p| 
              msg = p[:the_queue].pop :ack => p[:ack] 
              unless msg[:payload] == :queue_empty
                begin
                  start_time = Time.new
                  p[:block].call msg 
                  duration = (Time.new - start_time).to_f * 1000
                  p[:the_queue].ack if p[:ack] 
                  @success_handler.each do |h|
                    h.call :msg => msg, :queue => p[:queue], :exchange => p[:exchange], :topic => msg[:topic], :duration => duration
                  end
                rescue Exception => e
                  # to catch realy all exceptions
                  @error_handler.each do |h|
                    h.call :msg => msg, :error => e, :queue => p[:queue], :exchange => p[:exchange]
                  end
                end
                got_a_msg = true
              end
            end 
            while !@sync_queue.empty? 
              blck = @sync_queue.pop(true)
              blck.call
              got_a_msg = true
            end 
            #sleep 0.01 unless got_a_msg # wait 100ms if all queues are empty
          end
        rescue => e
          #  we probably lost the connection to the queue 
          # the next_host call at the beginning will select the next host
  
          sys_event :error, "we received exception #{e.inspect}, we switch to next rabbit host and reconnect. Backtrace: #{e.backtrace}"
          sleep 1
          retry unless @stopped
        end
        client.stop
      end
  
      def stop
        @stopped = true
      end
  
      private
  
      def next_host(settings)
        if settings[:hosts]
          settings[:host] = settings[:hosts].shift
          settings[:hosts] << settings[:host]
        end
        if settings[:ports]
          settings[:port] = settings[:ports].shift
          settings[:ports] << settings[:port]
        end
        raise ArgumentError.new "Host address is empty" if settings[:host].nil?
        raise ArgumentError.new "Port is empty" if settings[:port].nil?
      end
  
      def sys_event status, msg
        @sysevent_handler.each do |h|
          h.call status, msg 
        end
      end
  
    end
  end
end 
