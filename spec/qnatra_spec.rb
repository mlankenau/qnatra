require 'rubygems'
require "lib/qnatra/processor"
require 'rspec'
require 'bunny'

class TestProcessorWithAck < BaseProcessor

  process :exchange => "qnatra.test.exchange", :queue => "qnatra.test.queue", :ack => true, :key=>"*" do |msg|
    raise "something went wrong"
  end    

end

describe BaseProcessor do

  before do
    client = Bunny.new
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

  it "should requeue messages, that raise an exeception" do
    client = Bunny.new
    client.start

    ex = client.exchange('qnatra.test.exchange', :type => :topic)
    ex.publish("content dsnt matter", :key => 'mail')

    queue = client.queue('qnatra.test.queue')
    
    sleep 2 

    client.stop
    client = Bunny.new
    client.start
    queue = client.queue('qnatra.test.queue')

    msg = queue.pop
    msg[:payload].should eq("content dsnt matter")

    msg = queue.pop
    msg[:payload].should eq(:queue_empty)

    client.stop
  end

end
