module ActiveRecord::Turntable
  class Mixer
    class Fader
      class SpecifiedShard < Fader
        def execute
          shard, query = @shards_query_hash.first
          @proxy.with_shard(shard) do
            rest = @args.slice(0..1)
            keyrest = @args.slice(2..-1)&.each_with_object({}) { |item, result| result[item.keys.first] = item.values.first } || {}
            shard.connection.send(@called_method, query, *rest, **keyrest, &@block)
          end
        end
      end
    end
  end
end
