require 'rubygems'
require "lib/qnatra/processor"
require 'rspec'
require 'bunny'


rabbit_host = ENV['RABBIT_HOST'] || 'localhost'
rabbit_port = ENV['RABBIT_PORT'] || 5672
bunny_settings = { :host => rabbit_host, :port => rabbit_port }

class TestProcessorWithAck < BaseProcessor

  system_event do |msg|
    puts "SYSTEM_EVENT: #{msg}"
  end

  process :exchange => "qnatra.test.exchange", :queue => "qnatra.test.queue", :ack => true, :key=>"*" do |msg|
    raise "something went wrong"
  end    

end

describe BaseProcessor do

  before do
    client = Bunny.new bunny_settings 
    client.start
    queue = client.queue('qnatra.test.queue')
    begin
      msg = queue.pop
    end while msg[:payload] != :queue_empty
    client.stop

    @thread = Thread.new do
      TestProcessorWithAck.start :ack => true, :host => rabbit_host, :port => rabbit_port
    end
  end

  after do
    @thread.kill
  end

  it "should requeue messages, that raise an exeception" do
    client = Bunny.new bunny_settings
    client.start

    ex = client.exchange('qnatra.test.exchange', :type => :topic)
    ex.publish("content dsnt matter", :key => 'mail')

    sleep 1 
    TestProcessorWithAck.stop
    sleep 1

    queue = client.queue('qnatra.test.queue')

    msg = queue.pop
    msg[:payload].should eq("content dsnt matter")

    msg = queue.pop
    msg[:payload].should eq(:queue_empty)

    client.stop
  end

end



describe BaseProcessor do

  before do
    client = Bunny.new bunny_settings
    client.start
    queue = client.queue('qnatra.test.queue')
    begin
      msg = queue.pop
    end while msg[:payload] != :queue_empty
    client.stop

    @thread = Thread.new do
      TestProcessorWithAck.start
    end
  end

  after do
    @thread.kill
  end

  it "should not requeue messages" do
    client = Bunny.new bunny_settings
    client.start

    ex = client.exchange('qnatra.test.exchange', :type => :topic)
    ex.publish("content dsnt matter", :key => 'mail')

    sleep 1 
    TestProcessorWithAck.stop
    sleep 1

    queue = client.queue('qnatra.test.queue')
    
    msg = queue.pop
    msg[:payload].should eq(:queue_empty)

    client.stop
  end

end


