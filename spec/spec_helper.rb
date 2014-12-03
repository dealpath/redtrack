require 'bundler/setup'

require 'coveralls'
Coveralls.wear!

Bundler.setup

require 'redtrack' # and any other gems you need

RSpec.configure do |config|
  # some (optional) config here
end