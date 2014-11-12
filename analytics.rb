# encoding: UTF-8
require 'csv'
require 'ostruct'
require 'yaml'
require 'mathn'
require 'set'

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
    reduce(&:+)
  end
  
  def print_leaders_by(n, &f)
  end
  
  def print(n)
    Print.new(self_, n)
  end
  
  class Print
    
    def initialize(self_, n)
      @self_ = self_
      @n = n
    end
    
    def leaders_by(&f)
      r = @self_.to_a.
        map { |nick, data| [nick, f.(data)] }.
        sort_by { |nick, val| -val }
      r.take(@n).each { |nick, val| puts "#{nick}: #{val.to_percent_string}" }
      puts "Остальные: #{r.drop(@n).map { |nick, val| val }.sum.to_percent_string}"
    end
    
  end
  
end

class Numeric
  
  def to_percent_string
    "#{(self * 100).to_f.round(1).to_s}%"
  end
  
end

def mapreduce
  CSV.foreach(ARGV[0]) do |row|
    row_id = row[0].to_i
    msg = row[1]
    nick = row[2]
    yield row_id, msg, nick
  end
end

# Map-reduce: Collect aliases
# aliases = YAML.load <<YAML
#   Civeticious: Civet
#   Дуг: doug
#   ส็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็็: rejjin
#   rejjin__: rejjin
#   theromanzes: Romanzes
#   Uncatchable: Мариса
#   Vnuchaev: Lavir
#   cofreely_arrowized: rgtbctlpx
#   box: bx
#   yaskhan: Yaskhan
#   nancoil: Мариса
#   kuku: rgtbctlpx
#   апач-ультра: zxc
#   Дуг Ерохин: doug
#   Razor Ramon: zxc
#   GL_CULL_FACE: Мариса
#   маша: Мариса
#   poi: iop
#   Arzor Armon: zxc
#   bf: bx
#   doug': doug
#   boxxy-fag: bx
#   mynameiswinner: sw
#   middlelayerpes: doug
#   z-b: zxc
#   МЖМска: 7000p
#   7000р: 7000p
#   z-b☆☆☆☆: zxc
#   poi519: iop
#   -z-: zxc
#   Старшенькая: Мариса
#   zxc.betelgeuse: zxc
#   Цивет под снегом: Civet
#   coding@conference.jabber.ru: rejjin
# YAML
aliases = YAML.load <<DATA
  # alias: nick
  Civeticious: Цивет
  Цивет под снегом: Цивет
  Civet: Цивет
  poi: iop
  bf: bx
  nancoil: Марислава
  boxxy-fag: bx
  "1 swap 1 max 1+ 2 ?do i * loop": rgtbctlpx
  Обито: bx
  GL_CULL_FACE: Марислава
  Старшенькая: Марислава
  z-b☆☆☆☆: zxc
  Razor Ramon: zxc
  
DATA
nicks = Set.new(aliases.values)
ignored_renamings = [
  ["bx", "rejjin"],
  ["poi", "rgtbctltpx"],
  ["nancoil", "Rr~"],
  ["Rr~", "Марислава"],
  ["rejjin", "Yaskhan"],
  ["poi", "rejjin"]
]
ignored_renamings += ignored_renamings.map(&:reverse)
mapreduce do |row_id, msg, sender|
  if sender == "" and /^coding@conference.jabber.ru\/(.*) \-\> (.*)$/ === msg then
    old_alias = $1
    new_alias = $2
    next if [old_alias, new_alias].in? ignored_renamings
    old_nick = aliases[old_alias] || nicks[old_alias]
    new_nick = aliases[new_alias] || nicks[new_alias]
#     STDERR.puts %(info: #{old_alias}#{if old_nick.known? then " (#{old_nick})" end} → #{new_alias}#{if new_nick.known? then " (#{new_nick})" end})
    if    not old_nick.known? and not new_nick.known?
      nick = old_alias
      nicks.add nick
      aliases[new_alias] = nick
    elsif not old_nick.known? and     new_nick.known?
      aliases[old_alias] = new_nick
    elsif     old_nick.known? and not new_nick.known?
      aliases[new_alias] = old_nick
    elsif     old_nick.known? and     new_nick.known?
      if old_nick != new_nick
        STDERR.puts %(warning: someone tries to invade other's nick: #{old_alias} (#{old_nick}) → #{new_alias} (#{new_nick}))
      end
    end
  end
end
nicks_intersection = (aliases.keys & aliases.values)
if not nicks_intersection.empty? then
  STDERR.puts "Псевдонимы"
  STDERR.puts aliases.to_yaml
  raise %(карта псевдонимов не нормализована; следующие ники одновременно являются псевдонимами для других ников: #{nicks_intersection.join(", ")})
end
puts "Ники"
puts nicks.to_a.to_yaml
exit

# Map-reduce: Collect analytics
results = Hash.new do |hash, nick|
  hash[nick] = Hash.new { |hash, key| hash[key] = 0 }
end
max_row_id = 0
row_index = 0
mapreduce do |row_id, msg, nick|
#  break if row_index >= 1000
  nick = aliases[nick] || nick
  max_row_id = [row_id, max_row_id].max
  next if nick == ""
  results[nick]["messages"] += 1
  results[nick]["messages size"] += msg.size
  row_index += 1
end
total_messages = results.map { |_, data| data["messages"] }.sum
total_messages_size = results.map { |_, data| data["messages size"] }.sum

# Report
n = 10
puts "По количеству сообщений"
results.print(n).leaders_by { |data| data["messages"] / total_messages }
puts
puts "По объему сообщений"
results.print(n).leaders_by { |data| data["messages size"] / total_messages_size }
puts
puts "Последнее сообщение: #{max_row_id}"
