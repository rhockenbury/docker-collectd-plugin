#
# Collectd plugin for collecting docker container stats
#

import docker
import dateutil.parser
import json
import time
import sys
import os

if __name__ != "__main__":
    import collectd
else:
    class ExecCollectdValues:
        def dispatch(self):
            if not getattr(self, "host", ""):
                self.host = os.environ.get("COLLECTD_HOSTNAME", "localhost")
            identifier = "%s/%s" % (self.host, self.plugin)
            if getattr(self, "plugin_instance", ""):
                identifier += "-" + self.plugin_instance
            identifier += "/" + self.type
            if getattr(self, "type_instance", ""):
                identifier += "-" + self.type_instance
            print "PUTVAL", identifier, \
                  ":".join(map(str, [int(self.time)] + self.values))

    class ExecCollectd:
        def Values(self):
            return ExecCollectdValues()

        def error(self, msg):
            print "ERROR:", msg

        def warning(self, msg):
            print "WARNING:", msg

        def info(self, msg):
            print "INFO:", msg

    collectd = ExecCollectd()


class DockerClient(docker.Client):
    def stats(self, container):
        u = self._url("/containers/%s/stats" % container["Id"])
        response = self.get(u, stream=True)
        for line in response.iter_lines():
            if line:
                yield json.loads(line)


class Stats:
    @classmethod
    def emit(cls, container, type, value, t="", type_instance=""):
        val = collectd.Values()
        val.plugin = "docker"
        val.plugin_instance = container
        if type:
            val.type = type
        if type_instance:
            val.type_instance = type_instance
        val.values = value
        if time:
            val.time = time.mktime(dateutil.parser.parse(t).timetuple())
        else:
            val.time = time.time()
        val.dispatch()

    @classmethod
    def read(cls, container, stats):
        raise Exception("NotImplemented")


class CpuStats(Stats):
    @classmethod
    def read(cls, container, stats, t):
        #defaults
        total_cont_usage = 0.0
        system_cpu_usage = 0.0
        prev_total_cont_usage = 0.0
        prev_system_cpu_usage = 0.0
        cpu_percent = 0.0
        percpu_usage = []

        try: 
            total_cont_usage = float(stats["cpu_usage"]["total_usage"] or 0)
            system_cpu_usage = float(stats["system_cpu_usage"] or 0)
            percpu_usage = stats["cpu_usage"]["percpu_usage"]
        except ValueError:
            collectd.warning("Unable to parse memory stats - using defaults")
        except AttributeError:
            raise Exception("Invalid stats property access - check changes " + 
                "to docker stats endpoint")
        
        # get previous stats from file, and save latest stats
        cont_name = container["Names"][0] or container["Id"]
        stats_file = "/etc/collectd/stats/" + cont_name + "-cpu.stats"
        with open(stats_file, "w+") as f:
            prev_usage = f.read().split('\n')
            f.seek(0)
            output = "%s\n%s" % (total_cont_usage, system_cpu_usage)
            f.write(output) 

        if len(prev_usage) == 2:
            try: 
                prev_total_cont_usage = float(prev_usage[0])
                prev_system_cpu_usage = float(prev_usage[1])
            except ValueError:
                collectd.warning("Could not parse previous cpu usage " +
                                    "metrics from file - using defaults")
        else:
            collectd.warning("Previous cpu usage metrics not found in " + 
                                "file - using defaults")
        
        total_cont_delta = total_cont_usage - prev_total_cont_usage
        system_cpu_delta = system_cpu_usage - prev_system_cpu_usage 

        if total_cont_delta >= 0.0 and system_cpu_delta > 0.0:
            cpu_percent = (total_cont_delta / system_cpu_delta) * len(percpu_usage) * 100.0

        values = [ total_cont_usage, system_cpu_usage, cpu_percent ]
        cls.emit(container["Names"][0] or container["Id"], "cpu.usage", values, t=t)


class NetworkStats(Stats):
    @classmethod
    def read(cls, container, stats, t):
        #defaults
        rx_bytes = 0
        rx_errors = 0
        tx_bytes = 0
        tx_errors = 0

        try: 
            rx_bytes = int(stats["rx_bytes"] or 0)
            rx_errors = int(stats["rx_errors"] or 0)
            tx_bytes = int(stats["tx_bytes"] or 0)
            tx_errors = int(stats["tx_errors"] or 0)
        except ValueError:
            collectd.warning("Unable to parse memory stats - using defaults")
        except AttributeError:
            raise Exception("Invalid stats property access - check changes " + 
                "to docker stats endpoint")

        values = [rx_bytes, rx_errors, tx_bytes, tx_errors]
        cls.emit(container["Names"][0] or container["Id"], "network.usage", values, t=t)


class MemoryStats(Stats):
    @classmethod
    def read(cls, container, stats, t):
        # defaults
        usage = 0.0
        limit = 0.0
        percent = 0.0

        try: 
            usage = float(stats["usage"] or 0)
            limit = float(stats["limit"] or 0)
            percent = (usage / limit) if limit > 0.0
        except ValueError:
            collectd.warning("Unable to parse memory stats - using defaults")
        except AttributeError:
            raise Exception("Invalid stats property access - check changes " + 
                "to docker stats endpoint")

        values = [usage, limit, percent]
        cls.emit(container["Names"][0] or container["Id"], "memory.usage", values, t=t)


class DockerPlugin:
    CLASSES = {"network": NetworkStats,
               "cpu_stats": CpuStats,
               "memory_stats": MemoryStats
          }

    BASE_URL = "unix://var/run/docker.sock"
    BASE_STATS = "/usr/share/collectd/stats/"

    def configure_callback(self, conf):
        for node in conf.children:
            if node.key == 'BaseURL':
                self.BASE_URL = node.values[0]
            elif node.key == "BaseStats":
                self.BASE_STATS = node.values[0]
            else: 
                collectd.warning("Unrecognized conf parameter %s - ignoring" % node.values[0])

    def init_callback(self):
        self.client = DockerClient(base_url=self.BASE_URL)

    def read_callback(self):
        for container in self.client.containers():
            if not container["Status"].startswith("Up"):
                continue
            stats = self.client.stats(container).next()
            t = stats["read"]
            for key, value in stats.items():
                klass = self.CLASSES.get(key)
                if klass:
                    klass.read(container, value, t)


plugin = DockerPlugin()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        plugin.BASE_URL = sys.argv[1]
        plugin.BASE_STATS = sys.argv[2]
    plugin.init_callback()
    plugin.read_callback()

else:
    collectd.register_config(plugin.configure_callback)
    collectd.register_init(plugin.init_callback)
    collectd.register_read(plugin.read_callback)
