# encoding: UTF-8
require 'csv'
require 'ostruct'
require 'yaml'
require 'mathn'
require 'set'
require 'time'

#### COMMON LIBRARY ####

# Comment out/uncomment content of specific method to disable/enable
# particular messages.
module Logging

  def debug_info(msg)
#     STDERR.puts "debug: #{msg}"
  end

  def info(msg)
#     STDERR.puts "info: #{msg}"
  end

  def warning(msg)
    STDERR.puts "warning: #{msg}"
  end
  
  alias warn warning
  
end

include Logging

def error(msg)
  abort %(error: #{msg})
end

class Object
  
  def in? array
    array.include? self
  end
  
  def known?
    not nil?
  end
  
end

class Set
  
  # returns +value+ if this Set #include? value. Otherwise it returns nil.
  def [](value)
    if self.include? value then return value else return nil; end
  end
  
end

module Enumerable
  
  def sum
    reduce(&:+) or 0
  end
  
  def reduce_after(n, &f)
    self.take(n) + [self.drop(n).reduce(&f)]
  end
  
end

class Numeric
  
  def to_percent_string
    "#{(self * 100).to_f.round(1).to_s}%"
  end
  
  def to_info_size_string
    powers = ["", "k", "M", "G", "T"]
    power = lambda { powers.first }
    x = self
    while x > 1024
      x /= 1024
      powers.shift
    end
    "#{if x.is_a? Integer then x else x.to_f.round(1) end}#{power.()}"
  end
  
end

#### CHARTING ####

# +charts+ are objects returned by #chart().
def html(*charts)
  <<-HTML
    <html>
      <head>
        <meta charset="utf-8" />
        <!--Load the AJAX API-->
        <script type="text/javascript" src="https://www.google.com/jsapi"></script>
        <script type="text/javascript">
          // Load the Visualization API and the piechart package.
          google.load('visualization', '1.0', {'packages':['corechart']});
          // Set a callback to run when the Google Visualization API is loaded.
          google.setOnLoadCallback(drawCharts);
          // Callback that creates and populates a data table,
          // instantiates the pie chart, passes in the data and
          // draws it.
          function drawCharts() {
            #{charts.map(&:js).join("\n")}
          }
        </script>
      </head>
      <body>
        #{charts.map { |chart| %{<div id="#{chart.div_id}"></div>} }.join("\n")}
      </body>
    </html>
  HTML
end

# returns a chart object.
# 
# +data+ is map from nick to value.
# 
def pie(title, x_axis_title, y_axis_title, data)
  result = OpenStruct.new
  result.div_id = "div#{result.__id__.to_s}"
  result.js = <<-JS
    var data = new google.visualization.DataTable();
    data.addColumn('string', #{x_axis_title.inspect});
    data.addColumn('number', #{y_axis_title.inspect});
    data.addRows([
      #{data.map { |key, value| "[#{key.inspect}, #{value}]" }.join(", ")}
    ]);
    // Set chart options
    var options = {'title':'#{title}',
                   'width': 1280,
                   'height':800};
    // Instantiate and draw our chart, passing in some options.
    var chart = new google.visualization.PieChart(document.getElementById("#{result.div_id}"));
    chart.draw(data, options);
  JS
  return result
end

#### BUSINESS LOGIC ####

def mapreduce
  row_index = 0
  CSV.foreach(ARGV[0]) do |row|
    break if row_index >= 10000
    row_id = row[0].to_i
    msg = row[1]
    sender = row[2]
    datetime = Time.parse(row[3])
    yield row_id, msg, sender, datetime
    row_index += 1
  end
end

# Pass 1: Collect nicks an aliases.
#   Note. Each person has one nick. Each nick may have one or more aliases.
#   Each nick is alias for itself.
alias_to_nick = nil; begin
  # Add known aliases.
  alias_to_nick = YAML.load <<-YAML
    # alias: nick
    Цивет под снегом: Цивет
    Civeticious: Цивет
    апач-ультрас: zxc
    rejjin__: rejjin
    rgtbctltpx: rgtbctlpx
    Uncatchable: Марислава
    box: bx
    yaskhan: Yaskhan
    nancoil: Марислава
    marmalmad: Марислава
    Обито: bx
    kuku: rgtbctlpx
    Razor Ramon: zxc
    poi: iop
    GL_CULL_FACE: Марислава
    bf: bx
    Civet: Цивет
    boxxy-fag: bx
    Moby Dick: bx
    Reptiloid: Чубака
    z-b☆☆☆☆: zxc
    Старшенькая: Марислава
    huj: iop
    huy: iop
    Wizard Joe: Чубака
    Wormhole: Марислава
    z-b: zxc
    sw: mynameiswinner
    tallman: Чубака
    /s/tallman: Чубака
    лолита-ультрас: лолита
    lolita: лолита
    lola: лолита
    лола: лолита
    апач: zxc
    theromanzes: Romanzes
    amaH: aman
    zxc.betelgeuse: zxc
    -z-w-: zxc
    -z-m-: zxc
    rejjin_jru: rejjin
    Nice One: Марислава
    Untouchable: Марислава
    fakenimus\\40jabber.ru: fakenimus
    Fano plane matroid: Марислава
    pyos: doug
    anoos: doug
    чУбака: Чубака
    чубака: Чубака
    zb: zxc
    boxxyfag: bx
    rejjin@ж.ру: rejjin
    doug@xmpp.ru: doug
    rejjin@jabber.ru: rejjin
    rejjin_жру: rejjin
    rejjin@жру: rejjin
    marisa: Марислава
    Áɱǻƞ: aman
    bain the robot hero jesus king: bain
    Ксюша Скобчак: Марислава
    Yandere Pies: Pies
    Ramzessus: Romanzes
    Box: bx
    режин: rejjin
    GL_TEAPOT_SURFACE: Марислава
    Полигональная_прорубь: Марислава
    Пёс_среднего_уровня: doug
    middlelayerpes: doug
    Королева Ночи: Марислава
    anoosdog: doug
    rmnd@jabber.ru: rmnd
    akirayamaoka: Akira Yamaoka
    z-b/m: zxc
    zxc1@conference.jabber-br.org: zxc
    aman7: aman
    -z-w: zxc
    anoos_sweetshare: doug
    yobayoba: yoba
    AkiraYamaoka: Akira Yamaoka
    кодер-шмодер: Yaskhan
    7000p: Yaskhan
    7000р: Yaskhan
    зелёное_дерево: зелёное дерево
    rem22963: rem23
    "doug ": doug
    # from zxc
    Nice Two: Марислава
    rastan: pacman
    Madness Returns: Марислава
    ibex: zxc
    ll~~>: zxc
    rrr: zxc
    разящее лезвие: chair
  YAML
  # Each nick is alias to itself.
  alias_to_nick.values.each { |nick| alias_to_nick[nick] = nick }
  # Collect the rest of nicks and aliases.
  maybe_add_orphan_alias = lambda do |alias_|
    if not alias_to_nick.has_key? alias_
      debug_info %(#{alias_} = ???)
      alias_to_nick[alias_] = nil
    end
  end
  mapreduce do |row_id, msg, sender|
    if sender == ""
      if /^coding@conference.jabber.ru\/(.*) \-\> (.*)$/ === msg then
        old_alias = $1
        new_alias = $2
        old_nick = alias_to_nick[old_alias]
        new_nick = alias_to_nick[new_alias]
        debug_info %(#{old_alias} → #{new_alias})
        if    not old_nick.known? and not new_nick.known?
          nick = old_alias
          debug_info %(new nick: #{nick})
          alias_to_nick[nick] = nick
          debug_info %(#{new_alias} = #{nick})
          alias_to_nick[new_alias] = nick
        elsif not old_nick.known? and     new_nick.known?
          debug_info %(#{old_alias} = #{new_nick})
          alias_to_nick[old_alias] = new_nick
        elsif     old_nick.known? and not new_nick.known?
          debug_info %(#{new_alias} = #{old_nick})
          alias_to_nick[new_alias] = old_nick
        elsif     old_nick.known? and     new_nick.known?
          if old_nick != new_nick
            warning %(#{old_nick} tries to invade nick of #{new_nick}: #{old_alias} → #{new_alias})
          end
        end
      elsif /^coding@conference.jabber.ru\/(.*) has joined$/ === msg or
          /^coding@conference.jabber.ru\/(.*) has left$/ === msg or
          /^coding@conference.jabber.ru\/(.*) left$/ === msg then
        maybe_add_orphan_alias.($1)
      else
        warning %(unknown service message: #{msg})
      end
    elsif sender == "<coding@conference.jabber.ru" then
      # ignore
    else
      maybe_add_orphan_alias.(sender)
    end
  end
  # Orphan aliases are likely to be nicks.
  alias_to_nick.each do |alias_, nick|
    if nick == nil then
      alias_to_nick[alias_] = alias_
    end
  end
  # Print nicks.
  info %(nicks:)
  alias_to_nick.values.uniq.each { |nick| info "  #{nick}" }
end

# Pass 2: Collect analytics
analytics = Hash.new do |hash, nick|
  hash[nick] = Hash.new { |hash, key| hash[key] = 0 }
end
max_row_id = 0
min_datetime = nil
max_datetime = nil
mapreduce do |row_id, msg, sender, datetime|
  # Filter out unneeded elements.
  next if sender == "" or sender == "<coding@conference.jabber.ru"
  # Process!
  nick = alias_to_nick[sender] or error("sender is unknown: #{sender}")
  max_row_id = [row_id, max_row_id].max
  analytics[nick]["messages"] += 1
  analytics[nick]["messages size"] += msg.size
#   min_datetime = [min_datetime, datetime].compact.min
#   max_datetime = [max_datetime, datetime].compact.max
end

# Report
n = 25
rest_as_1st_and_sum_2nd = lambda do |result, row|
  ["Остальные", result[1] + row[1]]
end
sum_2nd = lambda do |analytics|
  analytics.map { |_, value| value }.sum
end
p1 = pie(
  "Количество сообщений",
  "Ник", "Сообщений",
  analytics.
    map { |nick, data| [nick, data["messages"]] }.
    sort_by { |_, messages| -messages }.
    reduce_after(n, &rest_as_1st_and_sum_2nd)
)
p2 = pie(
  "Объем сообщений",
  "Ник", "Объем",
  analytics.
    map { |nick, data| [nick, data["messages size"]] }.
    sort_by { |_, msgs_size| -msgs_size }.
    reduce_after(n, &rest_as_1st_and_sum_2nd)
)
puts html(p1, p2)
# puts "Период: с #{min_datetime.to_date.to_s} по #{max_datetime.to_date.to_s}"
