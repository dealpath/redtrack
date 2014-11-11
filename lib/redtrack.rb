# Copyright (c) 2014 RedHotLabs, Inc.
# Licensed under the MIT License

# Dependent requires
require 'logger'
require 'aws-sdk'
require 'json'
require 'pg'
require 'time'

# Require all of redtrack library
require 'redtrack_client'
require 'redtrack_kinesisclient'
require 'redtrack_loader'
require 'redtrack_local_file_stream'
require 'redtrack_datatypes'