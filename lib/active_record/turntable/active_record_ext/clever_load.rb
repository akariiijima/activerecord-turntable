module ActiveRecord::Turntable::ActiveRecordExt
  module CleverLoad
    extend ActiveSupport::Concern

    included do
      class << ActiveRecord::Base
        delegate :clever_load!, :to => :scoped
      end
    end

    def clever_load!(association_name)
      # load records
      records = self.to_a
      klass = records.first.class
      reflection = klass.reflections[association_name]

      if reflection
        foreign_class = reflection.klass
        foreign_objects = case reflection.macro
                          when :has_one
                            foreign_class.where(reflection.foreign_key => records.map(&reflection.association_primary_key.to_sym).uniq)
                          when :belongs_to
                            foreign_class.where(reflection.association_primary_key => records.map(&reflection.foreign_key.to_sym).uniq)
                          else
                            []
                          end

        self.each do |obj|
          matched_object = case reflection.macro
                           when :has_one
                             foreign_objects.find {|fo|
                               obj.send(reflection.association_primary_key) == fo.send(reflection.foreign_key)
                             }
                           when :belongs_to
                             foreign_objects.find {|fo|
                               obj.send(reflection.foreign_key) == fo.send(reflection.association_primary_key)
                             }
                           end
          obj.association(association_name).target = matched_object
          obj.association(association_name).send(:set_inverse_instance, matched_object)
        end
      end
      records
    end
  end
end
