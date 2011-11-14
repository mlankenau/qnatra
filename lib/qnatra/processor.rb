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

    # define a process
    def process(args, &block)
      @processes ||= [] 
      @processes << args.merge({:block => block })
    end


    # start execution
    def start
      client = Bunny.new()
      client.start

      @processes.each do |p| 
        exchange = client.exchange( p[:exchange], :type => :topic )
        queue = client.queue( p[:queue] )
        queue.bind(exchange, :key => p[:key])
        p[:the_queue] = queue
      end 

      # ensure all handler arrays are initialized
      @error_handler   ||= []
      @success_handler ||= []
      @processes       ||= []

      # endless loop and pop queues
      while true
        got_a_msg = false
        @processes.each do |p| 
          msg = p[:the_queue].pop
          unless msg[:payload] == :queue_empty
            begin
              start_time = Time.new
              p[:block].call msg 
              duration = (Time.new - start_time).to_f * 1000
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
    end
  end
end

