require 'narray'
require 'gsl'

class Sequel::Dataset
  def vectorize options={}

    result = {}
    axis = options[:axis]

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

    axis ? _process(result, axis) : result

  end

  private
  def _process data, axis
    axis_col = axis[:column]
    step = axis[:step]
    range = axis[:range]
    interpolate = axis[:interpolate]

    new_size = (range.last - range.first)/step.to_f
    raw_axis = data[axis_col]

    interp = GSL::Interp.alloc("linear", raw_axis.size) if interpolate

    data[axis_col] = NArray.float(new_size).indgen!(range.first,step)
    
    data.each do |k,v|

      next if k == axis_col 

      # if first is a float, vector is a NArray and will be interpolated
      if v[0].is_a? Float then
        if interpolate
          data[k] = interp.init(raw_axis, v).eval(raw_axis, v, data[axis_col])
        else
          data[k] = NArray.float(new_size)
          data[k][(raw_axis - range.first)/step.to_f] = v
        end
      end
    end
    
  end

end
