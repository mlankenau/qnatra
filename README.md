Qnatra
======


About
-----

Qnatra provides a very thin layer above an amqp client library (bunny). It let you as a developer focus on the handling of messages 
received from the queue and don't have to worry about how to access the queue and handle threading.


How to use
----------

Example


  class Processor <  BaseProcessor

    error do |args|
      puts "Error #{args[:error]} for message: #{args[:msg][:payload]}" 
    end

    success do |args|
      puts "we processed a message from #{args[:queue]} and routing key #{args[:msg][:delivery_details=][:routing_key]}")
    end

    process :exchange => "my_exchange", :queue => "my_queue", :key=>"a_routeing_key" do |msg|
      msg_text = msg[:payload]
      puts "processing message......"
      # todo: implement processing :)
    end

  end


  Processor.start



