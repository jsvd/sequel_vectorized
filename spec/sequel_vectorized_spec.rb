require 'rubygems'
require 'spec'
require 'sequel'

describe Sequel::Dataset do

  before :all do

    @DB = Sequel::sqlite

    @DB.create_table :items do
      text :name, :unique => true, :null => false
      boolean :active, :default => true
      column :price, :float
    end

    @DB.create_table :events do
      float :value
      float :ts
    end

    @items = @DB[:items]

    @items << {:name => 'abc', :price => 97}
    @items << {:name => 'def', :price => 11, :active => false}
    @items << {:name => 'ghi', :price => 18}

    @events = @DB[:events]

    @events << {:value => 2.2, :ts => Time.local(2008,1,1,12,30).to_f}
    @events << {:value => 1.1, :ts => Time.local(2008,1,1,13,30).to_f}
    @events << {:value => 3.3, :ts => Time.local(2008,1,1,14,30).to_f}

  end

  it "doesn't complain when including in Sequel::Dataset class" do
    lambda {
      require 'lib/sequel_vectorized'
    }.should_not raise_error
  end

  it "transforms numeric columns to narray" do

    ret = @events.select(:value).vectorize
    ret[:value].should be_an_instance_of NArray
    ret[:value].should == NArray.to_na([2.2, 1.1, 3.3])

  end

  it "creates narray of floats for any kind of numeric array" do

    ret = @events.vectorize
    ret.each {|k,v| v.should be_an_instance_of NArray }
    ret.should == { 
      :value  => NArray.to_na([2.2, 1.1, 3.3]),
      :ts     => NArray.to_na([1199190600.0, 1199194200.0, 1199197800.0])
    }

  end

  it "creates narray of bytes from an array of booleans" do 

    ret = @items.select(:active).vectorize
    ret[:active].should == NArray.to_na([1,0,1])

  end

  # spec an example filter
  it "is possible to vectorize the result set of a filter" do
    @events.filter(:value > 2).vectorize.should == {
      :value => NArray.to_na([2.2, 3.3]),
      :ts => NArray.to_na([Time.local(2008,1,1,12,30).to_f, Time.local(2008,1,1,14,30).to_f])
    }
  end

  it "is possible to pass an :axis option" do
    axis = {
      :column => :ts,
      :step => 60,
      :range => Time.local(2008,1,1,12).to_f ... Time.local(2008,1,1,15).to_f,
      :interpolate => true
    }

    lambda {
      @events.vectorize axis
    }.should_not raise_error
  end

  it "returns new narrays of size (range.last-range.first)/step if :axis is given" do

    axis = {
      :column => :ts,
      :step => 60,
      :range => Time.local(2008,1,1,12).to_f ... Time.local(2008,1,1,15).to_f,
      :interpolate => false
    }

    ret = @events.vectorize :axis => axis

    new_ret = ret.delete_if {|k,v| k.to_s.match(/__\w+/) }

    new_ret.should == {
      :value => NArray.to_na([[0]*30, 2.2, [0]*59, 1.1, [0]*59,3.3, [0]*29].flatten),
      :ts => NArray.float(180).indgen(Time.local(2008,1,1,12).to_f,60)
    }
  end

  it "interpolates NArrays if :axis option :interpolate is true" do
    # TODO: not spec'ed
    axis = {
      :column => :ts,
      :step => 60,
      :range => Time.local(2008,1,1,12).to_f ... Time.local(2008,1,1,15).to_f,
      :interpolate => true
    }

    ret = @events.vectorize :axis => axis

    ret[:ts].should == NArray.float(180).indgen(Time.local(2008,1,1,12).to_f,60)

  end


  it "returns a vectorized result set that inherits from Hash and has dot notation" do

    axis = {
      :column => :ts,
      :step => 60,
      :range => Time.local(2008,1,1,12).to_f ... Time.local(2008,1,1,15).to_f,
      :interpolate => false
    }

    ret = @events.vectorize :axis => axis

    ret.should be_a_kind_of Hash

    ret.should respond_to :ts
    ret.should respond_to :value

  end

  it "should be empty if there is no data" do
    @items.filter(:name => 'be').vectorize.should be_empty
    @items.filter(:name => 'be').vectorize(:axis => {:column => :price, :range => 0 ... 100, :step => 20 }).should be_empty
    @items.filter(:name => 'abc').vectorize(:axis => {:column => :price, :range => 0 ... 100, :step => 20 }).should_not be_empty
  end

  it "should not raise error if there's not enough data for interpolation" do

    axis = {
      :column => :ts,
      :step => 60,
      :range => Time.local(2008,1,1,12,30).to_f .. Time.local(2008,1,1,12,31).to_f,
      :interpolate => true
    }

    lambda { @events.vectorize :axis => axis }.should_not raise_error
    @events.vectorize(:axis => axis).should include :__value
    @events.vectorize(:axis => axis).should include :__raw_mask
    @events.vectorize(:axis => axis)[:__value].size.should == 1
    @events.vectorize(:axis => axis)[:__raw_mask].size.should == 2

  end

  it "should not raise error for an interval" do

    @events << {:value => 2.2, :ts => Time.local(2009,8,14,21,0).to_f}
    @events << {:value => 2.2, :ts => Time.local(2009,8,14,22,0).to_f}
    @events << {:value => 1.1, :ts => Time.local(2009,8,14,23,20).to_f}
    @events << {:value => 3.3, :ts => Time.local(2009,8,15,0).to_f}
    @events << {:value => 3.3, :ts => Time.local(2009,8,15,1).to_f}

    axis = {
      :column => :ts,
      :step => 60,
      :range => Time.local(2009,8,14,22).to_f .. Time.local(2009,8,15,0).to_f,
      :interpolate => false
    }

    lambda { @events.vectorize :axis => axis }.should_not raise_error
    axis[:interpolate] = true
    lambda { @events.vectorize :axis => axis }.should_not raise_error

    axis[:range] = Time.local(2009,8,14,22).to_f .. Time.local(2009,8,15,0).to_f

    lambda { @events.vectorize :axis => axis }.should_not raise_error
    axis[:interpolate] = false
    lambda { @events.vectorize :axis => axis }.should_not raise_error

  end
end
