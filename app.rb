require 'bundler'
Bundler.require

require 'json'

class App < Sinatra::Base
  configure do
    enable :logging
  end

  configure :development do
    register Sinatra::Reloader
  end

  get '/competitions/:id.json' do
    content_type :json

    id = params[:id]
    cache_key = "competition:#{id}"
    timestamp_cache_key = "competition:#{id}:timestamp"
    lock_key = "competition:#{id}:lock"

    redis_pool.with do |redis|
      data = redis.call('GET', cache_key)
      last_updated_at_value = redis.call('GET', timestamp_cache_key)
      last_updated_at = last_updated_at_value ? Time.parse(last_updated_at_value) : nil

      promise = nil
      should_update_cache = last_updated_at.nil? || (Time.now - last_updated_at) > cache_duration
      if should_update_cache
        lock_info = lock_manager.lock(lock_key, 60000)

        if lock_info
          promise = Concurrent::Promise.execute do
            data = fetch_competition(id)

            redis.call('SET', cache_key, data)
            redis.call('SET', timestamp_cache_key, Time.now.iso8601)

            data
          rescue => ex
            puts "Error: #{ex}"
          ensure
            lock_manager.unlock(lock_info)
          end
        end
      end

      result = if data
        data
      else
        promise&.value
      end

      json(JSON.parse(result))
    end
  end

  get '/test' do
    sleep 2
    json(timestamp: Time.now.iso8601)
  end

  private

  def fetch_competition(id)
    competition = Sutazekarate::Competition.new(id:)

    promises = competition.categories.map do |category|
      [
        category.async.competitors,
        category.async.ladder,
      ].flatten
    end.flatten

    promises.map(&:value)

    JSON.pretty_generate(competition.as_json(
      include: {
        categories: {
          include: {
            competitors: {},
            ladder: {
              include: {
                stages: {
                  include: :pairs
                }
              }
            },
          }
        },
      }
    ))
  end

  def redis_config
    @redis_config ||= RedisClient.config(url: ENV.fetch('REDIS_URL', 'redis://localhost:6379'))
  end

  def redis_pool
    @redis ||= redis_config.new_pool(timeout: 3, size: ENV.fetch('PUMA_THREADS', 20).to_i)
  end

  def lock_manager
    Redlock::Client.new([redis_config.new_client], retry_count: 0)
  end

  def cache_duration
    ENV.fetch('CACHE_DURATION', 60).to_i
  end
end
