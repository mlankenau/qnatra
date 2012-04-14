require 'spec_helper'

rabbit_host = ENV['RABBIT_HOST'] || 'localhost'
rabbit_port = ENV['RABBIT_PORT'] || 5672
bunny_settings = { :host => rabbit_host, :port => rabbit_port }

class TestProcessorWithMultithreading < Qnatra::Processor

  system_event do |msg|
    puts "SYSTEM_EVENT: #{msg}"
  end

  def self.work_queue 
    @@wq ||= WorkQueue.new(10)
  end

  outbound :sample_outbound, 'qnatra.test.exchange', :topic

  process :exchange => "qnatra.test.exchange", :queue => "qnatra.test.queue", :ack => false, :key=>"*" do |msg|
    work_queue.enqueue_b do 
      puts "worker received #{msg[:payload]}"
      ensync do
        post(:sample_outbound, msg[:payload], :key => 'mail')
      end
    end
  end   

end


describe TestProcessorWithMultithreading do

  before(:each) do
    @thread = Thread.new do
      #TestProcessorWithMultithreading.start_worker
      TestProcessorWithMultithreading.start :host => rabbit_host, :port => rabbit_port
    end

    client = Bunny.new bunny_settings
    client.start
    ex = client.exchange('qnatra.test.exchange', :type => :topic)
    100.times do |i|
      ex.publish("work packet #{i}", :key => 'mail')
    end
    client.stop
  end


  it "shoult work all 100 pakets" do
    sleep 300 
  end

end
