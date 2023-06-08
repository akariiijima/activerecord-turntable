module ActiveRecord::Turntable
  class Mixer
    class Fader
      class SelectShardsMergeResult < Fader
        def execute
          results = @shards_query_hash.map do |shard, query|
            args = @args.dup
            args[1] = args[1].dup if args[1].present?
            rest = @args.slice(0..1)
            keyrest = @args.slice(2..-1)&.each_with_object({}) { |item, result| result[item.keys.first] = item.values.first } || {}
            shard.connection.send(@called_method, query, *rest, **keyrest, &@block)
          end
          merge_results(results)
        end

        private

          def merge_results(results)
            if results.any? { |r| r.is_a?(ActiveRecord::Result) }
              first_result = results.find(&:present?)
              return results.first unless first_result

              ActiveRecord::Result.new(
                first_result.columns,
                results.flat_map(&:rows),
                first_result.column_types
              )
            else
              results.compact.inject(&:+)
            end
          end
      end
    end
  end
end
