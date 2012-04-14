require 'spec_helper'

rabbit_host = ENV['RABBIT_HOST'] || 'localhost'
rabbit_port = ENV['RABBIT_PORT'] || 5672
bunny_settings = { :host => rabbit_host, :port => rabbit_port }


class TestProcessorWithOutbound < Qnatra::Processor
  outbound :sample_outbound, 'sample_outbound_exchange', :topic

  process :exchange => "qnatra.test.exchange", :queue => "qnatra.test.queue", :key=>"*" do |msg|
    puts "hurra, got a msg"
    post(:sample_outbound, msg[:payload], :key => 'mail')
    puts "finished sending"
  end    
end

describe TestProcessorWithOutbound do
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

  it "it should forward a message to the outbound port" do
    @thread = Thread.new do
      begin
        TestProcessorWithOutbound.start :host => rabbit_host, :port => rabbit_port
      rescue => e
        puts "error in processor: #{e}"
      end
    end
    
    client = Bunny.new bunny_settings
    client.start

    dest_ex = client.exchange('sample_outbound_exchange', :type => :topic)
    dest_queue = client.queue('sample_outbound_queue')
    dest_queue.bind(dest_ex, :key => '*')

    ex = client.exchange('qnatra.test.exchange', :type => :topic)
    ex.publish("something to forward", :key => 'mail')

    sleep 3 
    TestProcessorWithOutbound.stop
    sleep 3

    msg = dest_queue.pop
    msg[:payload].should eq("something to forward")

    client.stop
  end
end

