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

  get '/up' do
    'OK'
  end

  get '/competitions.json' do
    content_type :json

    json(using_cache(:competition, :all) do
      fetch_competitions
    end)
  end

  get '/competitions/:id/export.json' do
    content_type :json

    id = params[:id]
    json(using_cache(:competition_export, id) do
      fetch_competition_export(id)
    end)
  end

  get '/competitions/:id/categories.json' do
    content_type :json

    id = params[:id]
    json(using_cache(:competition_categories, id) do
      fetch_competition_categories(id)
    end)
  end

  get '/categories/:id/export.json' do
    content_type :json

    id = params[:id]
    json(using_cache(:category_export, id) do
      fetch_category_export(id)
    end)
  end

  private

  def fetch_competitions
    competitions = Sutazekarate::Competition.all

    JSON.pretty_generate(competitions.as_json)
  end

  def fetch_competition_export(id)
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

  def fetch_competition_categories(id)
    competition = Sutazekarate::Competition.new(id:)

    categories = competition.categories

    JSON.pretty_generate(categories.as_json)
  end

  def fetch_category_export(id)
    category = Sutazekarate::Category.new(id:)

    promises = [
      category.async.competitors,
      category.async.ladder,
    ].flatten

    promises.map(&:value)

    JSON.pretty_generate(category.as_json(
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
    ))
  end

  def using_cache(prefix, id)
    logger.info("Fetching #{prefix}:#{id}")
    cache_key = "#{prefix}:#{id}"
    timestamp_cache_key = "#{prefix}:#{id}:timestamp"
    lock_key = "#{prefix}:#{id}:lock"

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
            data = yield

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

      JSON.parse(result)
    end
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
