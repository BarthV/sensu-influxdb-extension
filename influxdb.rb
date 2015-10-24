require 'rubygems' if RUBY_VERSION < '1.9.0'
require 'influxdb'
require 'json'

module Sensu::Extension
  class InfluxDB < Handler
    def name
      definition[:name]
    end

    def definition
      {
        type: 'extension',
        name: 'influxdb'
      }
    end

    def description
      'Outputs metrics to InfluxDB'
    end

    def post_init()
      # NOTE: Making sure we do not get any data from the Main
    end

    def run(event_data)
      event = parse_event(event_data)
      conf = parse_settings()

      # init data and check settings
      data = []
      client = event['client']['name']
      event['check']['influxdb']['database'] ||= conf['database']

      influx_conf = {
        :database => event['check']['influxdb']['database'],
        :username => conf['username'],
        :password => conf['password'],
        :time_precision => event['check']['time_precision'],
        :use_ssl => conf['use_ssl'],
        :verify_ssl => conf['verify_ssl'],
        :async => true,
        :retry => conf['retry']
      }

      if conf['hosts']
        influx_conf.merge!({:hosts => conf['hosts']})
      else
        influx_conf.merge!({:host => conf['host']})
      end

      event['check']['output'].split(/\n/).each do |line|
        key, value, time = line.split(/\s+/)
        values = {:value => value.to_f}

        if event['check']['duration']
          values.merge!({:duration => event['check']['duration'].to_f})
        end

        if conf['strip_metric'] == 'host'
          key = slice_host(key, client)
        elsif conf['strip_metric']
          key.gsub!(/^.*#{conf['strip_metric']}\.(.*$)/, '\1')
        end

        # Avoid things break down due to comma in key name
        # TODO : create a key_clean def to refactor this
        key.gsub!(',', '\,')

        # Merging : default conf tags < check embedded tags < sensu client/host tag
        tags = conf.fetch('tags', {}).merge(event['check']['influxdb']['tags']).merge({:host => client})

        data += [{:series => key, :tags => tags, :values => values, :timestamp => time.to_i}]
      end

      begin
        influxdb = ::InfluxDB::Client.new influx_conf
        influxdb.write_points(data);0
      rescue
        puts 'Failed to send data to InfluxDB'
      end

      yield('', 0)
    end

    def stop
      true
    end

    private
    def parse_event(event_data)
      begin
        event = JSON.parse(event_data)

        # default values
        event['check']['time_precision'] ||= 's' # n, u, ms, s, m, and h (default community plugins use standard epoch date)
        event['check']['influxdb'] ||= {}
        event['check']['influxdb']['tags'] ||= {}
        event['check']['influxdb']['database'] ||= nil

      rescue => e
        puts "Failed to parse event data: #{e}"
      end
      return event
    end

    def parse_settings()
      begin
        settings = @settings['influxdb']
        
        # default values
        settings['tags'] ||= {}
        settings['use_ssl'] ||= false
        settings['verify_ssl'] ||= false
        settings['retry'] ||= 10

      rescue => e
        puts "Failed to parse InfluxDB settings #{e}"
      end
      return settings
    end

    def slice_host(slice, prefix)
      prefix.chars.zip(slice.chars).each do |char1, char2|
        if char1 != char2
          break
        end
        slice.slice!(char1)
      end
      if slice.chars.first == '.'
        slice.slice!('.')
      end
      return slice
    end
  end
end
