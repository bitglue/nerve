require 'nerve/reporter/base'
require 'etcd'

class Nerve::Reporter
  class Etcd < Base
    def initialize(service)
      %w{etcd_host instance_id host port}.each do |required|
        raise ArgumentError, "missing required argument #{required} for new service watcher" unless service[required]
      end
      @host = service['etcd_host']
      @port = service['etcd_port'] || 4003
      path = service['etcd_path'] || '/'
      @key = path.split('/').push(service['instance_id']).join('/')
      @data = parse_data({'host' => service['host'], 'port' => service['port'], 'name' => service['instance_id']})
      @full_key = nil
    end

    def start()
      log.info "nerve: connecting to etcd at #{@host}:#{@port}"
      @etcd = ::Etcd.client(:host => @host, :port => @port)
      log.info "nerve: successfully created etcd connection to #{@host}:#{@port}"
    end

    def stop()
       report_down
       @etcd = nil
    end

    def report_up()
      etcd_save
    end

    def report_down
      etcd_delete
    end

    def update_data(new_data='')
      @data = parse_data(new_data)
      etcd_save
    end

    def ping?
      @etcd and !!@etcd.leader
    end

    private

    def etcd_delete
      return unless @etcd and @full_key
      begin
        @etcd.delete(@key)
      rescue ::Etcd::KeyNotFound
      rescue Errno::ECONNREFUSED
      end
    end

    def etcd_create
      @full_key = @etcd.create_in_order(@key, :value => @data).key
      log.info "registered at etcd path #{@full_key} with value #{@data}"
    end

    def etcd_save
      return etcd_create unless @full_key
      @etcd.set(@full_key, :value => @data, :ttl => 30)
      log.info "updated etcd path #{@full_key} with value #{@data}"
    end
  end
end

