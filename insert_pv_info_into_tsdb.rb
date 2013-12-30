#!/home/work/router/env/ruby192/bin/ruby
require "eventmachine"
require "socket"
interval_sec=60 #note: please don't change
nginx_log_path="/home/work/router/log/nginx_router_access.log"
tsdb_host="10.36.58.31"
tsdb_port="8242"
hostname=`hostname`.to_s.chomp

#tsdb connection
class TsdbConnection < EventMachine::Connection
    def connection_completed
        puts ("Connected to TSDB server")
        @port, @ip = Socket.unpack_sockaddr_in(get_peername)
    end
    def unbind
        if @port && @ip
            puts ("Lost connection to TSDB server, reconnecting")
            EM.add_timer(1.0) do
                begin
                    reconnect(@ip, @port)
                rescue EventMachine::ConnectionError => e
                    puts e
                    unbind
                end
            end
        else
            puts ("Couldn't connect to TSDB server, exiting.")
            exit!
        end
    end
end

class ProcessPv
    def initialize(tsdb_connection,hostname)
        @pv_response_time={}
        @pv_response_code={}
        @tsdb_connection=tsdb_connection
        @time_now=Time.now
        @hostname=hostname
    end

    #get pv info from router log
    def process_file(file_name)
    begin
        time=(@time_now-120).strftime("%Y:%H:%M")  # 2 min ago
        File.open(file_name,"rb") do |file|
            file.each_line do |line|
                if line=~/#{time}/ and info=/^(\S+)\s+-\s+\[[^\[\]]+\]\s+"[^"]+"\s+(\d+)\s+\d+\s+"[^"]+"\s+"[^"]+"\s+\S+\s+response_time:(\S+).*/.match(line)
                    uri=info[1].to_s.chomp
                    response_code=info[2]
                    response_time=(info[3].to_f*1000).to_i
                    @pv_response_time[uri]||=response_time
                    @pv_response_time[uri]=response_time if response_time>@pv_response_time[uri]
                    @pv_response_code[uri]||={}
                    @pv_response_code[uri][response_code]||=0
                    @pv_response_code[uri][response_code]+=1
                end
            end
        end
        rescue => e
            puts "Error in process_file: #{e.message}"
        end
    end

    #find file according to time
    def file_list(nginx_log_path)
        list=[]
        list.push(nginx_log_path)
        file_1hour_ago="#{nginx_log_path}.#{(Time.now-3600).strftime("%Y%m%d%H").to_s}"
        list.push(file_1hour_ago) if File.exist?(file_1hour_ago)
        list
    end

    def send_metric(name, time,value, tags = {})
        begin
            tags = tags.collect { |tag| tag.join("=") }.sort.join(" ")
            command = "put #{name} #{time} #{value} #{tags}\n"
            @tsdb_connection.send_data(command)
        rescue => e
            puts "error in send_metric: #{e.message}"
        end
    end

    #record data to tsdb
    def record_result_to_tsdb()
        log_time_sec=@time_now.to_i-120
        @pv_response_time.each do |uri,response_time|
            send_metric("jpaas_app_responsetime",log_time_sec,response_time,{"uri"=>uri,"router"=>@hostname})
        end
        @pv_response_code.each do |uri,response_code_hash|
            response_code_hash.each do |response_code,count|
                send_metric("jpaas_app_pv",log_time_sec,count,{"uri"=>uri,"response_code"=>response_code,"router"=>@hostname})
            end
        end
    end
end
EM.run do
    tsdb_connection=EventMachine.connect(tsdb_host,tsdb_port, TsdbConnection)
    hostname=`hostname`.to_s.chomp
    EM::PeriodicTimer.new(interval_sec) do
        EM.defer do
            process_log_everytime=ProcessPv.new(tsdb_connection,hostname)
            log_file_list=process_log_everytime.file_list(nginx_log_path)
            log_file_list.each do |file|
                process_log_everytime.process_file(file)
            end
            process_log_everytime.record_result_to_tsdb()
        end
    end	
end


