require 'narray'

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

    axis ? _process(result, axis) : result

  end

  private
  def _process data, axis
    column = axis[:column]
    range = axis[:range]
    step = axis[:step]

    data_places = data[column]/step.to_f - range.first/step.to_f
    data[column] = NArray.float((range.last - range.first)/step.to_f).indgen!(range.first,60)
    
    data.each do |k,v|
      next if k == column 
      if v.kind_of? NArray and v[0].kind_of? Numeric then
        data[k] = NArray.float((range.last - range.first)/step.to_f)
        data[k][data_places] = v
      end
    end

    data
  end

end
