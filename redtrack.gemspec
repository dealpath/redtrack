Gem::Specification.new do |s|
  s.name          = 'redtrack'
  s.version       = '0.0.1'
  s.date          = '2014-11-06'
  s.summary       = 'Real-time event tracking in AWS.'
  s.description   = 'System for real time event tracking & loading infrastructure for AWS. Utilizes Kinesis as a data broker and Redshift as a data warehouse.'
  s.authors       = ['Luke Rajlich']
  s.email         = 'lrajlich@gmail.com'
  s.files         = `git ls-files`.split("\n")
  s.require_paths = ["lib"]
  s.homepage      = 'https://github.com/redhotlabs/redtrack'
  s.license       = 'MIT'

  s.add_dependency 'aws-sdk'
  s.add_dependency 'pg'
end
