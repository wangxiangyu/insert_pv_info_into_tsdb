#!/home/work/router/env/ruby192/bin/ruby
require "eventmachine"
require "socket"
interval_sec=60 #note: please don't not change
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

#get pv info from router log
def process_file(file_name,time,pv_response_time,pv_response_code)
begin
    File.open(file_name,"rb") do |file|
        file.each_line do |line|
            if line=~/#{time}/ and info=/^(\S+)\s+-\s+\[[^\[\]]+\]\s+"[^"]+"\s+(\d+)\s+\d+\s+"[^"]+"\s+"[^"]+"\s+\S+\s+response_time:(\S+).*/.match(line)
                uri=info[1].to_s.chomp
                response_code=info[2]
                response_time=(info[3].to_f*1000).to_i
                if pv_response_time[uri].nil?
                    pv_response_time[uri]=response_time 
                else	
                    pv_response_time[uri]=response_time if response_time>pv_response_time[uri]
                end
                    pv_response_code[uri]={} if pv_response_code[uri].nil?
                if pv_response_code[uri][response_code].nil?
                    pv_response_code[uri][response_code]=0
                end
                pv_response_code[uri][response_code]+=1
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

def send_metric(tsdb_connection,name, time,value, tags = {})
    tags = tags.collect { |tag| tag.join("=") }.sort.join(" ")
    command = "put #{name} #{time} #{value} #{tags}\n"
    tsdb_connection.send_data(command)
end

#record data to tsdb
def record_result_to_tsdb(pv_response_time,pv_response_code,log_time_sec,tsdb_connection,hostname)
    pv_response_time.each do |uri,response_time|
        send_metric(tsdb_connection,"jpaas_app_responsetime",log_time_sec,response_time,{"uri"=>uri,"router"=>hostname})
    end
    pv_response_code.each do |uri,response_code_hash|
        response_code_hash.each do |response_code,count|
            send_metric(tsdb_connection,"jpaas_app_pv",log_time_sec,count,{"uri"=>uri,"response_code"=>response_code,"router"=>hostname})
        end
    end
end

EM.run do
    tsdb_connection=EventMachine.connect(tsdb_host,tsdb_port, TsdbConnection)
    EM::PeriodicTimer.new(interval_sec) do
        EM.defer do
            pv_response_time={}
            pv_response_code={}
            time_now=Time.now
            log_time_grep=(time_now-120).strftime("%Y:%H:%M")  # 2 min ago
            log_time_sec=time_now.to_i-120
            file_list(nginx_log_path).each do |file|
                process_file(file,log_time_grep,pv_response_time,pv_response_code)
            end
            record_result_to_tsdb(pv_response_time,pv_response_code,log_time_sec,tsdb_connection,hostname)
        end
    end	
end


