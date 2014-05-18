module MoSQL
  class SmartSchemaError < StandardError; end;

  class SmartSchema < Schema
    include MoSQL::Logging

    def self.get_type(col)
      case col
      when BSON::ObjectId
        "CHAR(36)"
      when String, NilClass
        "TEXT"
      when Integer, Fixnum
        "INTEGER"
      when Bignum,
        "BIGINTEGER"
      when TrueClass, FalseClass
        "BOOLEAN"
      when Float
        "DOUBLE PRECISION"
      when Array
        "JSON"
      else
        puts "You gave me #{col.class} -- I have no idea what to do with that."
      end
    end

    def self.recurse_obj(k,v,cols = [])
      v.each do |k1, v1|
        name = k + "_" + k1
        source = k + "." + k1
        if v1.is_a?(Hash)
          self.recurse_obj(name,v1,cols)
        else
          cols << { :source => source, :name => name, :type => self.get_type(v1) }
        end
      end
      return cols
    end

    def initialize(map, mongo)
      puts("Smart Schema...")
      @map = {}
      map.each do |dbname, db|
        @map[dbname] = { :meta => parse_meta(db[:meta]) }
        db.each do |cname, spec|
          @map[dbname][cname] = {:columns => [], :meta => parse_meta(spec[:meta])}
          first_item = mongo[dbname][cname].find_one()
          first_item.each do |k,v|
            if v.is_a?(Hash)
              @map[dbname][cname][:columns] = @map[dbname][cname][:columns] + self.class.recurse_obj(k,v)
            else
              @map[dbname][cname][:columns] << { :source => k, :name => k, :type => self.class.get_type(v) }
            end
          end
        end
      end
      puts(@map)
    end

  end
end
