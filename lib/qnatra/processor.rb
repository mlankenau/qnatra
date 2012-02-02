require 'rubygems'
require 'bunny'

#
# Baseclass to implement processors to process queue events (consumer role)
#
# define processing instructions in a sintra style like:
#    process :exchange => "public.mail.notifications", :queue => "mail.notification.processor1", :key=>"*" do |msg|
#       puts "received msg #{msg[:payload]}"
#    end
#
class BaseProcessor

  class << self
    

    def error(&block)
      @error_handler ||= []
      @error_handler << block
    end

    def success(&block)
      @success_handler ||= []
      @success_handler << block
    end

    def system_event(&block) 
      @sysevent_handler ||= []
      @sysevent_handler << block
    end

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

      sys_event 'startup'
      @stopped = false

      pop_settings = settings.reject { |k,v| k != :ack }

      begin
        next_host(settings)
        
        sys_event "opening client on #{settings[:host]}:#{settings[:port]}"
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
          queue.bind(exchange, :key => p[:key])
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
              rescue => e
                @error_handler.each do |h|
                  h.call :msg => msg, :error => e, :queue => p[:queue], :exchange => p[:exchange]
                end
              end
              got_a_msg = true
            end
          end 
          sleep 0.1 unless got_a_msg # wait 100ms if all queues are empty
        end
      rescue => e
        #  we probably lost the connection to the queue 
        # the next_host call at the beginning will select the next host

        sys_event "we received excpetion #{e.inspect}, we switch to next rabbit host and reconnect. Backtrace: #{e.backtrace}"
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

    def sys_event msg
      @sysevent_handler.each do |h|
        h.call msg 
      end
    end

  end
end

