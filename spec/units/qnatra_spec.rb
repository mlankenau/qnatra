require 'spec_helper'

rabbit_host = ENV['RABBIT_HOST'] || 'localhost'
rabbit_port = ENV['RABBIT_PORT'] || 5672
bunny_settings = { :host => rabbit_host, :port => rabbit_port }

class TestProcessorWithAck < Qnatra::Processor
  system_event do |msg|
    puts "SYSTEM_EVENT: #{msg}"
  end
  process :exchange => "qnatra.test.exchange", :queue => "qnatra.test.queue", :ack => true, :key=>"*" do |msg|
    raise "something went wrong"
  end    
end

class TestProcessorWithoutAck < Qnatra::Processor
  system_event do |msg|
    puts "SYSTEM_EVENT: #{msg}"
  end
  process :exchange => "qnatra.test.exchange", :queue => "qnatra.test.queue", :ack => false, :key=>"*" do |msg|
    raise "something went wrong"
  end    
end

describe Qnatra::Processor do

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

  it "should not start if there is not a valid host to connect to" do
    @thread = Thread.new do
      TestProcessorWithAck.start :port => rabbit_port, :host => "localhost"
    end

    client = Bunny.new bunny_settings
    client.start

    ex = client.exchange('qnatra.test.exchange', :type => :topic)
    ex.publish("content dsnt matter", :key => 'mail')

    sleep 3 
    TestProcessorWithAck.stop
    sleep 3 

    queue = client.queue('qnatra.test.queue')

    msg = queue.pop
    msg[:payload].should eq("content dsnt matter")

    client.stop
  end
  
  it "should requeue messages, that raise an exeception" do
    @thread = Thread.new do
      TestProcessorWithAck.start :host => rabbit_host, :port => rabbit_port
    end

    client = Bunny.new bunny_settings
    client.start

    ex = client.exchange('qnatra.test.exchange', :type => :topic)
    ex.publish("content dsnt matter", :key => 'mail')

    sleep 3 
    TestProcessorWithAck.stop
    sleep 3 

    queue = client.queue('qnatra.test.queue')

    msg = queue.pop
    msg[:payload].should eq("content dsnt matter")

    msg = queue.pop
    msg[:payload].should eq(:queue_empty)

    client.stop
  end

  it "should not requeue messages" do
    @thread = Thread.new do
      TestProcessorWithoutAck.start :host => rabbit_host, :port => rabbit_port
    end

    client = Bunny.new bunny_settings
    client.start

    ex = client.exchange('qnatra.test.exchange', :type => :topic)
    ex.publish("content dsnt matter", :key => 'mail')

    sleep 3 
    TestProcessorWithoutAck.stop
    sleep 3

    queue = client.queue('qnatra.test.queue')
    
    msg = queue.pop
    msg[:payload].should eq(:queue_empty)

    client.stop
  end
end

