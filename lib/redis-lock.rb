require "digest/sha1"
require "ostruct"
require "redis"
require "redis-lock/version"

class Redis

  ###
  # Set/release/extend locks on a single redis instance.
  # See http://redis.io/commands/set
  #
  class MultiLock

    class MultiLockNotAcquired < StandardError
    end

    @@config = OpenStruct.new(
      default_timeout: 10,
      default_life: 60,
      default_sleep: 125
    )

    attr_reader :redis
    attr_reader :keys
    attr_reader :owner
    attr_accessor :life     # how long we expect to keep this lock locked
    attr_accessor :logger

    HOST = `hostname`.strip

    LOCK_SCRIPT = <<EOS
local owner = ARGV[1]
if owner == nil then
  return -2
end

local expire_in = ARGV[2]
if expire_in == nil then
  return -1
end

for i, key in pairs(KEYS) do
  local exists = redis.call('exists', key)
  if exists == 1 then
    return false
  end
end

for i, key in pairs(KEYS) do
  redis.call('set', key, owner, 'PX', expire_in)
end
return true
EOS
    LOCK_SCRIPT_HASH = Digest::SHA1.hexdigest(LOCK_SCRIPT)

    RELEASE_SCRIPT = <<EOS
local owner = ARGV[1]
if owner == nil then
  return -2
end

local force = false
if owner == 0 and ARGV[2] == 1 then
  force = true
end

for i, key in pairs(KEYS) do
  local value = redis.call('get', key)
  if value == nil or (value ~= owner  and force == false) then
    return false
  end
end

for i, key in pairs(KEYS) do
  redis.call('del', key)
end
return true
EOS
    RELEASE_SCRIPT_HASH = Digest::SHA1.hexdigest(RELEASE_SCRIPT)

    EXTEND_SCRIPT = <<EOS
local owner = ARGV[1]
if owner == nil then
  return -2
end

local expire_in = ARGV[2]
if expire_in == nil then
  return -3
end

for i, key in pairs(KEYS) do
  local value = redis.call('get', key)
  if value == nil or (value ~= owner) then
    return false
  end
end

for i, key in pairs(KEYS) do
  redis.call('PEXPIRE', key, expire_in)
end
return true
EOS
    EXTEND_SCRIPT_HASH = Digest::SHA1.hexdigest(EXTEND_SCRIPT)

    # @param redis is a Redis instance
    # @param key is a unique string identifying the object to lock, e.g. "user-1"
    # @param options[:life] should be set, but defaults to 1 minute
    # @param options[:owner] may be set, but defaults to HOSTNAME:PID
    # @param options[:sleep] is used when trying to acquire the lock; milliseconds; defaults to 125.
    def initialize( redis, keys, options = {} )
      check_keys( options, :owner, :life, :sleep )
      @redis  = redis
      @keys    = [keys].flatten.collect{|key| "lock:#{key}" }
      @owner  = options[:owner] || "#{HOST}:#{Process.pid}"
      @life   = options[:life] || @@config.default_life
      @sleep_in_ms = options[:sleep] || @@config.default_sleep
    end

    # @param acquisition_timeout defaults to 10 seconds and can be used to determine how long to wait for a lock.
    def lock( acquisition_timeout = @@config.default_timeout, &block )
      do_lock_with_timeout(acquisition_timeout) or raise MultiLockNotAcquired.new(keys)
      if block then
        begin
          result = (block.arity == 1) ? block.call(self) : block.call
        ensure
          release_lock
        end
      end
      result
    end

    def extend_life( new_life )
      do_extend( new_life ) or raise MultiLockNotAcquired.new(keys)
      self
    end

    def unlock
      release_lock
      self
    end

    #
    # queries
    #

    def locked?( now = Time.now.to_i )
      redis.mget(keys).each do |value|
        return false if value != owner
      end
      true
    end

    #
    # internal api
    #

    private
    def do_lock_with_timeout( acquisition_timeout )
      locked = false
      with_timeout(acquisition_timeout) { locked = do_lock }
      locked
    end

    # @returns true if locked, false otherwise
    def do_lock
      begin
        redis.evalsha(
          LOCK_SCRIPT_HASH, keys: keys, argv: [owner, (life * 1000).to_i])
      rescue Redis::CommandError => e
        redis.eval(LOCK_SCRIPT,  keys: keys, argv: [owner, (life * 1000).to_i])
      end
    end

    def do_extend( new_life, my_owner = owner )
      begin
        redis.evalsha(
          EXTEND_SCRIPT_HASH, keys: keys, argv: [my_owner, (new_life * 1000).to_i])
      rescue Redis::CommandError => e
        redis.eval(EXTEND_SCRIPT, keys: keys, argv: [my_owner, (new_life * 1000).to_i])
      end
    end

    # Only actually deletes it if we own it.
    # There may be strange cases where we fail to delete it, in which case expiration will solve the problem.
    def release_lock( my_owner = owner )
      begin
        redis.evalsha(RELEASE_SCRIPT_HASH, keys: keys, argv: [my_owner])
      rescue Redis::CommandError => e
        redis.eval(RELEASE_SCRIPT, keys: keys, argv: [my_owner])
      end
    end

    # Calls block until it returns true or times out.
    # @param block should return true if successful, false otherwise
    # @returns true if successful, false otherwise
    # Note: at one time I thought of using a backoff strategy, but don't think that's important now.
    def with_timeout( timeout, &block )
      expire = Time.now + timeout.to_f
      sleepy = @sleep_in_ms / 1000.to_f()
      # this looks inelegant compared to while Time.now < expire, but does not oversleep
      loop do
        return true if block.call
        log :debug, "Timeout for #{@key}" and return false if Time.now + sleepy > expire
        sleep(sleepy)
        # might like a different strategy, but general goal is not use 100% cpu while contending for a lock.
      end
    end

    def log( level, *messages )
      if logger then
        logger.send(level) { "[#{Time.now.strftime "%Y%m%d%H%M%S"} #{oval}] #{messages.join(' ')}" }
      end
      self
    end

    def check_keys( set, *options )
      extra = set.keys - options
      raise "Unknown Option #{extra.first}" if extra.size > 0
    end

    def self.config
      @@config
    end
  end # Lock

  # Convenience methods

  # @param key is a unique string identifying the object to lock, e.g. "user-1"
  # @options are as specified for Redis::Lock#lock (including :life)
  # @param options[:life] should be set, but defaults to 1 minute
  # @param options[:owner] may be set, but defaults to HOSTNAME:PID
  # @param options[:sleep] is used when trying to acquire the lock; milliseconds; defaults to 125.
  # @param options[:acquire] defaults to 10 seconds and can be used to determine how long to wait for a lock.
  def multi_lock( keys, options = {}, &block )
    acquire = options.delete(:acquire) || 10
    Redis::MultiLock.new( self, keys, options ).lock( acquire, &block )
  end

  def multi_unlock( key )
    Redis::MultiLock.new( self, keys ).unlock
  end
end # Redis
