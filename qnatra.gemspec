# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "qnatra/version"

Gem::Specification.new do |s|
  s.name        = "qnatra"
  s.version     = Qnatra::VERSION
  s.authors     = ["Marcus Lankenau"]
  s.email       = ["marcus.lankenau@friendscout24.de"]
  s.homepage    = ""
  s.summary     = %q{Framework to implement servies based on AMQP inspired by Sinatra web framework}
  s.description = %q{Qnatra include does the plumming with bunny to operater you queues}

  s.rubyforge_project = "qnatra"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_dependency('bunny', '>= 0.7.9')

  # specify any dependencies here; for example:
  s.add_development_dependency "rspec"
  # s.add_runtime_dependency "rest-client"
end
