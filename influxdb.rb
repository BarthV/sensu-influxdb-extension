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

    def post_init
      # init InfluxDB connexion with global args
      conf = parse_settings
      influx_conf = {
        prefix: conf['prefix'],
        database: conf['database'],
        username: conf['username'],
        password: conf['password'],
        time_precision: conf['time_precision'],
        use_ssl: conf['use_ssl'],
        verify_ssl: conf['verify_ssl'],
        # async: true, DISABLED : BIG PERFORMANCE ISSUE ! queue size & workers hardcoded !
        retry: conf['retry']
      }

      if conf['hosts']
        influx_conf.merge!(hosts: conf['hosts'])
      else
        influx_conf.merge!(host: conf['host'])
      end

      @ixdb = ::InfluxDB::Client.new influx_conf
    end

    def run(event_data)
      event = parse_event(event_data)
      conf = parse_settings

      # init data and check settings
      data = []
      client = event['client']['name']

      # override database global attr with check attribute
      @ixdb.config.database = event['check']['influxdb']['database'] ||= conf['database']

      event['check']['output'].split(/\n/).each do |line|
        key, value, time = line.split(/\s+/)
        values = { value: value.to_f }

        if event['check']['duration']
          values.merge!(duration: event['check']['duration'].to_f)
        end

        if conf['strip_metric'] == 'host'
          key = slice_host(key, client)
        elsif conf['strip_metric']
          key.gsub!(/^.*#{conf['strip_metric']}\.(.*$)/, '\1')
        end

        # Avoid things break down due to special chars in key name
        # TODO : create a key_clean def to refactor this
        key.gsub!(',', '\,')
        key.gsub!(/\s/, '\ ')
        key.gsub!('"', '\"')
        key.gsub!("\\"){ "\\\\" }

        # Merging : default conf tags < check tags < sensu client/host tag
        # We're doing weird things to ensure that we're using only symbols as hashkey
        tags = conf['tags'].inject({}){|memo,(k,v)| memo[k.to_sym] = v; memo}
        tags.merge!(event['check']['influxdb']['tags'].inject({}){|memo,(k,v)| memo[k.to_sym] = v; memo})
        tags.merge!(host: client)

        data += [{ series: key, tags: tags, values: values, timestamp: time.to_i }]
      end

      begin
        @ixdb.write_points(data, event['check']['time_precision'])
      rescue
        @logger.warn("Failed to write points to InfluxDB: #{data.to_json}")
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
        # n, u, ms, s, m, and h (default community plugins use epoch in sec)
        event['check']['influxdb'] ||= {}
        event['check']['influxdb']['tags'] ||= {}

      rescue => e
        @logger.warn("Failed to parse event data: #{e}")
      end
      return event
    end

    def parse_settings
      begin
        settings = @settings['influxdb']

        # default values
        settings['tags'] ||= {}
        settings['use_ssl'] ||= false
        settings['verify_ssl'] ||= true
        settings['retry'] ||= 5
        settings['prefix'] ||= ''
        settings['time_precision'] ||= 's'

      rescue => e
        @logger.warn("Failed to parse InfluxDB settings #{e}")
      end
      return settings
    end

    def slice_host(slice, prefix)
      prefix.chars.zip(slice.chars).each do |char1, char2|
        break if char1 != char2
        slice.slice!(char1)
      end
      slice.slice!('.') if slice.chars.first == '.'
      return slice
    end
  end
end
