module MoSQL
  class SmartSchemaError < StandardError; end;

  class SmartSchema < Schema
    include MoSQL::Logging

    def get_type(col)
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
        when Time
          "TIMESTAMP"
        else
          puts "You gave me #{col.class} -- I have no idea what to do with that."
      end
    end

    def recurse_obj(k,v,cols = [])
      v.each do |k1, v1|
        name = k + "." + k1
        source = k + "." + k1
        if v1.is_a?(Hash)
          self.recurse_obj(name,v1,cols)
        else
          if v1.is_a?(Array) and @extract.include?(name)
            cols << { :source => source, :name => name, :type => "EXTRACT" }
          else
            cols << { :source => source, :name => name, :type => self.get_type(v1) }
          end
        end
      end
      return cols
    end

    def get_columns(obj,map,extract)
      cols = []
      @extract = extract
      obj.each do |k,v|
        if not map.has_key?(k)
          if v.is_a?(Array) and extract.include?(k)
            cols << { :source => k, :name => k, :type => "EXTRACT" }
          else
            if v.is_a?(Hash)
              cols = cols + self.recurse_obj(k,v)
            else
              cols << { :source => k, :name => k, :type => self.get_type(v) }
            end
          end
        else
          if map[k].has_key?(:source)
            s = map[k][:source]
          else
            s = k
          end
          cols << { :source => s, :name => k, :type => map[k][:type]}
        end
      end
      return cols
    end

    def transform_row(obj, schema)
      log.debug("Current Schema #{schema} for obj #{obj}")
      row = []
      schema[:columns].each do |col|

        source = col[:source]
        type = col[:type]

        if source.start_with?("$")
          v = fetch_special_source(obj, source)
        else
          v = fetch_and_delete_dotted(obj, source)
          case v
          when BSON::Binary, BSON::ObjectId, Symbol
            v = v.to_s
          when Hash, Array
            v = JSON.dump(v)
          end
        end
        row << v
      end
      row
    end

    def alter_schema(schema, alterations, db)
      log.info("Adding column(s) to table #{schema}: #{alterations}")
      db.send(:alter_table, schema) do
        alterations.each do |new_column|
          if new_column[:type] != "EXTRACT"
            add_column new_column[:name], new_column[:type]
          end
        end
      end
    end

    def transform(ns, obj, db, schema=nil)
      schema ||= find_ns!(ns)
      dbname,cname = ns.split(".")

      obj = obj.dup

      row = transform_row(obj,schema)
      if obj.keys.length > 0
        new_columns = get_columns(obj, @map_original[dbname][cname][:columns], @map_original[dbname][cname][:extract])
        @map[dbname][cname][:columns] = @map[dbname][cname][:columns] + new_columns
        row = row + transform_row(obj, {:columns => new_columns })
        alter_schema(schema[:meta][:table], new_columns, db)
      end

      log.debug { "Transformed: #{row.inspect}" }

      row
    end

    def initialize(map, mongo, learn)
      log.info("Using Smart Schema...")
      @map = {}
      @map_original = map
      if learn
        log.info("Smart Schema learning...")
        map.each do |dbname, db|
            @map[dbname] = { :meta => parse_meta(db[:meta]) }
            db.each do |cname, spec|
                @map[dbname][cname] = {:columns => [], :meta => parse_meta(spec[:meta]), :extract => spec[:extract]}
                first_item = mongo[dbname][cname].find_one()
                if first_item
                    @map[dbname][cname][:columns] = get_columns(first_item, map[dbname][cname][:columns], map[dbname][cname][:extract])
                end
            end
        end
      end
    end

  end
end
