require 'narray'

module Sequel
  module Plugins
    module Vectorized

      class Sequel::Dataset
        def vectorize 

          result = {}

          # transform dataset to hash of arrays
          map {|row| row.each{|att,value| (result[att] ||= []) << value}}

          # transform numeric and boolean arrays to narrays
          result.each do |k,v|

            first = v.first

            if first.kind_of?(Numeric) then

              v[0] = first.to_f # so NArray is always of type float
              result[k] = NArray.to_na v

            elsif first == true || first == false then

              result[k] = NArray.float(v.size)
              result[k][] = v.map {|i| (i == true) ? 1 : 0 }

            end
          end
        end
      end
    end
  end
end
