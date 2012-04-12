require 'bundler/gem_tasks'
require 'rspec/core/rake_task'
require 'yard'
require 'yard/rake/yardoc_task'

task :default => :spec

RSpec::Core::RakeTask.new(:spec)


namespace :doc do
  YARD::Rake::YardocTask.new :generate do |t|
    t.files   = ['lib/**/*.rb']
  end
end

