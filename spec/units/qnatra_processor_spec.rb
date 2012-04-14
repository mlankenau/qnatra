describe Qnatra::Processor do
  describe 'internal method' do
    describe 'duration_of' do
      [1, 3].each do |duration|
        it "should track #{duration * 1000}ms during #{duration}s" do
	  # Sorry, i don't see a better way, than just sleep some time, what makes the test quite slow.
	  Qnatra::Processor.send(:duration_of) { sleep(duration) }.to_i.should be_within(1).of(duration * 1000)
	end
      end
    end
  end
end
