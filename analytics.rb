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

known_aliases = YAML.load <<DATA
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
  huj: iop
  huy: iop
DATA
aliases = known_aliases
nicks = Set.new(aliases.values)
mapreduce do |row_id, msg, sender|
  if sender == "" and /^coding@conference.jabber.ru\/(.*) \-\> (.*)$/ === msg then
    old_alias = $1
    new_alias = $2
#     next if [old_alias, new_alias].in? ignored_renamings
    old_nick = aliases[old_alias] || nicks[old_alias]
    new_nick = aliases[new_alias] || nicks[new_alias]
    STDERR.puts %(info: #{old_alias} → #{new_alias})
    if    not old_nick.known? and not new_nick.known?
      nick = old_alias
      STDERR.puts %(info: new person: #{nick})
      nicks.add nick
      aliases[new_alias] = nick
    elsif not old_nick.known? and     new_nick.known?
      STDERR.puts %(info: #{old_alias} = #{new_nick})
      aliases[old_alias] = new_nick
    elsif     old_nick.known? and not new_nick.known?
      STDERR.puts %(info: #{new_alias} = #{old_nick})
      aliases[new_alias] = old_nick
    elsif     old_nick.known? and     new_nick.known?
      if old_nick != new_nick
        STDERR.puts %(warning: #{old_nick} tries to invade nick of #{new_nick}: #{old_alias} → #{new_alias})
      end
    end
  end
end
nicks_intersection = (aliases.keys & aliases.values)
if not nicks_intersection.empty? then
  STDERR.puts
  STDERR.puts "Псевдонимы"
  STDERR.puts aliases.to_yaml
  abort %(error: aliases map is not normal; following nicks are aliases too: #{nicks_intersection.join(", ")})
end
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
puts "ID последнего сообщения: #{max_row_id}"
