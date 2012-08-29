require 'spec_helper'

rabbit_host = ENV['RABBIT_HOST'] || 'localhost'
rabbit_port = ENV['RABBIT_PORT'] || 5672
bunny_settings = { :host => rabbit_host, :port => rabbit_port }

class TestProcessorWithMultirouting < Qnatra::Processor
  system_event do |status, msg|
    puts "SYSTEM_EVENT: (#{status}) #{msg}"
  end

  process :exchange => "qnatra.test.exchange", :queue => "qnatra.test.queue", :ack => true, :key => ["key_a", "key_b"] do |msg|
    puts "received: #{msg.inspect}"
    @@messages_received ||= 0
    @@messages_received += 1 
  end   

  def self.messages_received
    @@messages_received || 0
  end
end


describe  do

  before(:each) do
    client = Bunny.new bunny_settings 
    client.start
    queue = client.queue('qnatra.test.queue')
    begin
      msg = queue.pop
    end while msg[:payload] != :queue_empty
    client.stop
  end

  after(:each) do
    @thread.kill
  end

  it "should accept an array as routing key and bind to multiple routing keys" do
    @thread = Thread.new do
      begin
        TestProcessorWithMultirouting.start :host => "localhost", :port => rabbit_port
      rescue => e 
        puts e
      end
    end

    client = Bunny.new bunny_settings
    client.start

    ex = client.exchange('qnatra.test.exchange', :type => :topic)
    ex.publish("content dsnt matter", :key => 'key_a')
    ex.publish("content dsnt matter", :key => 'key_b')
    sleep 3 
    TestProcessorWithMultirouting.stop

    TestProcessorWithMultirouting.messages_received.should be 2

  end
  
end

