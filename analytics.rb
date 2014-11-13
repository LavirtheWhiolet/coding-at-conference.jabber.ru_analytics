# encoding: UTF-8
require 'csv'
require 'ostruct'
require 'yaml'
require 'mathn'
require 'set'

#### COMMON LIBRARY ####

# Comment out/uncomment content of specific method to disable/enable
# particular messages.
module Logging

  def debug_info(msg)
    STDERR.puts "debug: #{msg}"
  end

  def info(msg)
    STDERR.puts "info: #{msg}"
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
  
  def print(n)
    Print.new(self, n)
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

#### BUSINESS LOGIC ####

def mapreduce
  CSV.foreach(ARGV[0]) do |row|
    yield(row_id = row[0].to_i, msg = row[1], nick = row[2])
  end
end

# Pass 1: Collect nicks an aliases.
#   Note. Each person has one nick. Each nick may have zero or more aliases.
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
  tallman: /s/tallman
  лолита-ультрас: лолита
  lolita: лолита
  lola: лолита
  лола: лолита
DATA
aliases = known_aliases # .dup
nicks = Set.new(aliases.values)
orphan_aliases = Set.new
add_orphan_alias = lambda do |alias_|
  if not orphan_aliases.include? alias_
    debug_info %(orphan alias: #{alias_})
    orphan_aliases.add alias_
  end
end
mapreduce do |row_id, msg, sender|
  if sender == ""
    if /^coding@conference.jabber.ru\/(.*) \-\> (.*)$/ === msg then
      old_alias = $1
      new_alias = $2
      old_nick = aliases[old_alias] || nicks[old_alias]
      new_nick = aliases[new_alias] || nicks[new_alias]
      debug_info %(#{old_alias} → #{new_alias})
      if    not old_nick.known? and not new_nick.known?
        nick = old_alias
        debug_info %(new nick: #{nick})
        nicks.add nick
        debug_info %(#{new_alias} = #{nick})
        aliases[new_alias] = nick
      elsif not old_nick.known? and     new_nick.known?
        debug_info %(#{old_alias} = #{new_nick})
        aliases[old_alias] = new_nick
      elsif     old_nick.known? and not new_nick.known?
        debug_info %(#{new_alias} = #{old_nick})
        aliases[new_alias] = old_nick
      elsif     old_nick.known? and     new_nick.known?
        if old_nick != new_nick
          warning %(#{old_nick} tries to invade nick of #{new_nick}: #{old_alias} → #{new_alias})
        end
      end
    elsif /^coding@conference.jabber.ru\/(.*) has joined$/ === msg then
      # This may be a new person or an old person with a different alias.
      add_orphan_alias.($1)
    end
  elsif "<coding@conference.jabber.ru" === msg then
    # ignore
  else
    # We don't know if it is a new person or an old person with a different
    # alias.
    add_orphan_alias.(sender)
  end
end
if not (intersection = (aliases.keys & aliases.values)).empty? then
  info %(aliases:)
  aliases.each { |alias_, nick| info %(  #{alias_}: #{nick}) }
  error %(aliases map is not normal; following nicks are aliases too: #{nicks_intersection.join(", ")})
end
additional_nicks = orphan_aliases.subtract(aliases.keys)
nicks = nicks.union(additional_nicks)
info %(nicks:)
nicks.each { |nick| info "  #{nick}" }

# Pass 2: Collect analytics
results = Hash.new do |hash, nick|
  hash[nick] = Hash.new { |hash, key| hash[key] = 0 }
end
max_row_id = 0
row_index = 0
mapreduce do |row_id, msg, nick|
#   # Take n first elements.
#   break if row_index >= 1000
  # Filter out unneeded elements.
  next if nick == "" or nick == "<coding@conference.jabber.ru"
  # Process!
  nick = aliases[nick] || nicks[nick] || error("unknown nick #{nick}")
  max_row_id = [row_id, max_row_id].max
  results[nick]["messages"] += 1
  results[nick]["messages size"] += msg.size
  # 
  row_index += 1
end
total_messages = results.map { |_, data| data["messages"] }.sum
total_messages_size = results.map { |_, data| data["messages size"] }.sum

# Report
n = 10000
puts
puts "По количеству сообщений"
results.print(n).leaders_by { |data| data["messages"] / total_messages }
puts
puts "По объему сообщений"
results.print(n).leaders_by { |data| data["messages size"] / total_messages_size }
puts
puts "ID последнего сообщения: #{max_row_id}"
