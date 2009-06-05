require 'narray'
require 'gsl'

class Sequel::Dataset
  def vectorize options={}

    result = {}
    axis = (options[:axis] ||= {})

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

    # add dot notation
    columns.each do |col|
      result.instance_eval %Q{def #{col}; self[:#{col}]; end}
    end

    return result if (axis.empty? || result.empty?)

    _process(result, axis)

  end

  private
  def _process data, axis
    axis_col = axis[:column]
    step = axis[:step]
    range = axis[:range]
    interpolate = axis[:interpolate]

    new_size = (range.last - range.first)/step.to_f
    raw_axis = data[axis_col]
    data["__#{axis_col}".to_sym] = raw_axis

    interp = GSL::Interp.alloc("linear", raw_axis.size) if interpolate

    data[axis_col] = NArray.float(new_size).indgen!(range.first,step)

    data[:__raw_mask] = NArray.byte(new_size)
    data[:__raw_mask][(raw_axis - range.first)/step.to_f] = 1
    
    data.keys.each do |k|

      next if (k == axis_col || k == "__#{axis_col}".to_sym)

      v = data[k]

      # if first is a float, vector is a NArray and will be interpolated
      if v[0].is_a? Float then
        data["__#{k}".to_sym] = v #backup data in :__key
        if interpolate
          # problem with GSL lack of NArray support on compilation
          raw_axis_vector = GSL::Vector[raw_axis.to_a]
          v_vector = GSL::Vector[v.to_a]
          data_vector = GSL::Vector[data[axis_col].to_a]

          data[k] = interp.init(raw_axis_vector, v_vector)
          data[k] = NArray.to_na data[k].eval(raw_axis_vector, v_vector, data_vector).to_a
        else
          data[k] = NArray.float(new_size)
          data[k][(raw_axis - range.first)/step.to_f] = v
        end
      end
    end

    data
    
  end

end
