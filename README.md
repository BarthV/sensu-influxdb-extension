# Requirements

This extension uses [InfluxDB official gem](https://github.com/influxdb/influxdb-ruby) to send metrics in a asynchronous way.

You'll have to ensure that the following gem is present inside Sensu's embedded Ruby :
* `influxdb >= 0.2.0` ruby gem

# Usage
The configuration options are pretty straight forward.

Metrics are inserted into the database using the check's key name as _measurement_ name. So if you're using the `sensu-plugins-load-checks` community plugin, the output will be :
```
my-host-01.load_avg.one 0.02 1444824197
my-host-01.load_avg.five 0.04 1444824197
my-host-01.load_avg.fifteen 0.09 1444824197
```
In this example, you'll have 3 differents _measurements_ in your database :
```
> show measurements
name: measurements
------------------
name
my-host-01.load_avg.fifteen
my-host-01.load_avg.five
my-host-01.load_avg.one
```

```
> select * from "my-host-01.load_avg.one";
name: my-host-01.load_avg.one
------------------
time                  host        value  duration
2015-10-14T13:53:22Z  my-host-01  0.34   0.399 
2015-10-14T13:53:32Z  my-host-01  0.29   0.419
2015-10-14T13:53:42Z  my-host-01  0.39   0.392
2015-10-14T13:53:52Z  my-host-01  0.41   0.398
[...]
```

Additionally a `duration` value can be present based on the time it took the check to run (this is fetched from the sensu event data).

The name of the _measurement_ is based on the value of `strip_metric` described below.
The name of the _key_ `host` is grabbed from sensu event client name.

## Extension not a handler
Note that the first push of this was a handler that could be called via `pipe`. This is now an actual extension that's more performant since it's actually in the sensu-server runtime. Additionally it's now using batch submission to InfluxDB by writing all the points for a given series at once.

Just drop the ruby file in `/etc/sensu/extensions` and create a set to wrap this extension into a callable handler. In this example, we created a `metrics` handler wrapping a debug output and this Influx extension :

_/etc/sensu/conf.d/handlers/metrics.json_ :
```json
{
  "handlers": {
    "metrics": {
      "type": "set",
      "handlers": [ "debug", "influxdb"]
      }
    }
  }
}
```

Since Sensu 0.17.1 you can also run extensions directly without creating any wrapper handler :
```
Check definitions can now specify a Sensu check extension to run,
"extension", instead of a command.
```

## Handler config

_/etc/sensu/conf.d/influxdb.json_ :
```json
{
  "influxdb": {
    "database": "stats",
    "host": "localhost",
    "port": "8086",
    "username": "stats",
    "password": "stats",
    "use_ssl": false,
    "verify_ssl": false,
    "strip_metric": "somevalue",
    "tags": {
      "region": "my-dc-01",
      "stage": "prod"
    }
  }
}
```

### Config attributes

* `host`, `port`, `username`, `password` and `database` are pretty straight forward. If `use_ssl` is set to true, the connection to the influxdb server will be made using https instead of http.

* `tags` hash is also pretty straight forward. Just list here in a flat-hash design as many influxdb tags you wish to be added in your _measurements_.

* `strip_metric` however might not be. This is used to "cleanup" the data sent to influxdb. Normally everything sent to handlers is akin to the `graphite`/`stats` style:
```
  something.host.metrictype.foo.bar
or
  host.stats.something.foo.bar
```

Really the pattern is irrelevant. People have different tastes. Adding much of that data to the column name in InfluxDB is rather silly so `strip_metric` provides you with a chance to add a value that strips off everything up to (and including that value). This allows you to continue sending to graphite or statsd or whatever and still use this handler.

Using the examples above, if you set the `strip_metric` to `host`, then the column in InfluxDB would be called `metrictype.foo.bar` or `stats.something.foo.bar`. If you set the value to `foo` then the column would simply be called `foo`

Note that :
* `strip_metric` isn't required.
* you can cleanup an arbitrary string from your keyname or use `host` as special value to cleanup the sensu event client name from your key

#### More attributes

* `retry` : specify the retry number for the write operation (`false` for disable, `0` or true for infinite, any `integer` for a finite retry times. Default is `8`
* `prefix` : the specified path prefix when building the url (e.g.: /prefix/db/dbname...)

## Check options

In the check config, an optional `influxdb` section can be added, containing a `database` option and `tags`.
If specified, this values will overrides the default `database` option in the handler config and adds (or override if needed) influxdb tags.

This allows events to be written to different influxdb databases and modify key indexes on a check-by-check basis.

You can also adjust the timestamp time precision of your checks in the check config with the `time_precision` attribute :
* default is `'s'`
* accepted values are `'n'`, `'u'`, `'ms'`, `'s'`, `'m'` and `'h'`

### Example check config

`/etc/sensu/conf.d/checks/metrics-load.json`
```json
{
  "checks": {
    "metrics-load": {
      "type": "metric",
      "command": "metrics-load.rb",
      "standalone": true,
      "handlers": [
        "metrics"
      ],
      "interval": 60,
      "time_precision": "s",
      "influxdb": {
        "database": "custom-db",
        "tags": {
           "stage": "prod",
           "region": "eu-west-1"
        }
      }
    }
  }
}
```

_Result_ :
```
load_avg.one,stage:prod,region:eu-west-1,host:iprint-test-sa-01.photobox.com value=1.04,duration=0.402  1444816792147
load_avg.five,stage:prod,region:eu-west-1,host:iprint-test-sa-01.photobox.com value=0.86,duration=0.398 1444816792147
load_avg.fifteen,stage:prod,region:eu-west-1,host:iprint-test-sa-01.photobox.com value=0.84,duration=0.375 1444816792147

 * Will be sent to : http://my-influx09.company.com:8086/db/custom-db/series?time_precision=s&u=sensu&p=sensu
```
