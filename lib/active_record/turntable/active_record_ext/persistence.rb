module ActiveRecord::Turntable::ActiveRecordExt
  module Persistence
    ::ActiveRecord::Persistence.class_eval do
      def reload(options = nil)
        clear_aggregation_cache
        clear_association_cache

        finder_scope = if turntable_enabled? and self.class.primary_key != self.class.turntable_shard_key.to_s
                         self.class.where(self.class.turntable_shard_key => self.send(turntable_shard_key))
                       else
                         self.class
                       end

        fresh_object =
          if options && options[:lock]
            self.class.unscoped { finder_scope.lock.find(id) }
          else
            self.class.unscoped { finder_scope.find(id) }
          end

        @attributes.update(fresh_object.instance_variable_get('@attributes'))

        @column_types           = self.class.column_types
        @column_types_override  = fresh_object.instance_variable_get('@column_types_override')
        @attributes_cache       = {}
        self
      end

      def touch(name = nil)
        raise ActiveRecordError, "can not touch on a new record object" unless persisted?

        attributes = timestamp_attributes_for_update_in_model
        attributes << name if name

        unless attributes.empty?
          current_time = current_time_from_proper_timezone
          changes = {}

          attributes.each do |column|
            column = column.to_s
            changes[column] = write_attribute(column, current_time)
          end

          changes[self.class.locking_column] = increment_lock if locking_enabled?

          @changed_attributes.except!(*changes.keys)
          primary_key = self.class.primary_key

          finder_scope = if turntable_enabled? and primary_key != self.class.turntable_shard_key.to_s
                           self.class.unscoped.where(self.class.turntable_shard_key => self.send(turntable_shard_key))
                         else
                           self.class.unscoped
                         end

          finder_scope.where(primary_key => self[primary_key]).update_all(changes) == 1
        end
      end
    end

    ::ActiveRecord::Persistence.class_eval do
      private

      def relation_for_destroy
        pk         = self.class.primary_key
        column     = self.class.columns_hash[pk]
        substitute = self.class.connection.substitute_at(column, 0)
        klass      = self.class

        relation = self.class.unscoped.where(
                                             self.class.arel_table[pk].eq(substitute))
        if klass.turntable_enabled? and klass.primary_key != klass.turntable_shard_key.to_s
          relation = relation.where(klass.turntable_shard_key => self.send(turntable_shard_key))
        end

        relation.bind_values = [[column, id]]
        relation
      end

      def update_record(attribute_names = @attributes.keys)
        attributes_with_values = arel_attributes_with_values_for_update(attribute_names)
        if attributes_with_values.empty?
          0
        else
          klass = self.class
          column_hash = klass.connection.schema_cache.columns_hash klass.table_name
          db_columns_with_values = attributes_with_values.map { |attr,value|
            real_column = column_hash[attr.name]
            [real_column, value]
          }
          bind_attrs = attributes_with_values.dup
          bind_attrs.keys.each_with_index do |column, i|
            real_column = db_columns_with_values[i].first
            bind_attrs[column] = klass.connection.substitute_at(real_column, i)
          end
          condition_scope = klass.unscoped.where(klass.arel_table[klass.primary_key].eq(id_was || id))
          if klass.turntable_enabled? and klass.primary_key != klass.turntable_shard_key.to_s
            condition_scope = condition_scope.where(klass.turntable_shard_key => self.send(turntable_shard_key))
          end
          stmt = condition_scope.arel.compile_update(bind_attrs)
          klass.connection.update stmt, 'SQL', db_columns_with_values
        end
      end
    end
  end
end
