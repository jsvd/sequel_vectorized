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

end
