module MoSQL
  class SmartSchemaError < StandardError; end;

  class SmartSchema < Schema
    include MoSQL::Logging

    def get_type(col)
      case col
        when BSON::ObjectId
          "CHAR(24)"
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
          "TIMESTAMP WITH TIME ZONE"
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
            cols << get_extract(k, v)
          else
            cols << { :source => source, :name => name, :type => self.get_type(v1) }
          end
        end
      end
      return cols
    end

    def get_columns(obj,map=nil,extract=nil)
      cols = []
      extract = parse_extract(extract)
      map = parse_columns(map)
      @extract = extract
      obj.each do |k,v|
        if not map.has_key?(k)
          if v.is_a?(Array) and extract.include?(k)
            cols << get_extract(k, v, map)
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

    def get_extract(k,v, map)
      name = @cname + "." + k
      col = { :source => k, :name => name, :type => "EXTRACT" }
      if v.size() > 0
        if map.has_key?(k.to_sym)
          col[:schema] = get_columns(v[0], map=map[k.to_sym])
        else
          col[:schema] = get_columns(v[0])
        end
      end
      return col
    end

    def process_extract(col, obj, pid, db)
      log.debug("Processing extract for #{col} for #{obj} for parent #{pid}")
      create_extract_table(col, db)
      obj[col[:source]].each do |o|
        row = [pid]
        col[:schema].each do |c|
          if o.has_key?(c[:source].split(".")[0])
            source = c[:source].split(".")
            v=o
            while source.size() > 0
              v = v[source[0]]
              source.delete(source[0])
            end
            case v
              when BSON::Binary, BSON::ObjectId, Symbol
                v = v.to_s
              when Hash, Array
                v = JSON.dump(v)
              when nil
                v = nil
              else
                v = v.to_s
            end
            row << v
            obj.delete(c)
          else
            row << nil
          end
        end
        db[col[:name].to_sym].insert(row)
      end
    end

    def create_extract_table(col, db)
      db.send(:create_table?, col[:name]) do
        column "pid", "CHAR(36)"
        col[:schema].each do |col|
          if col[:type] != "EXTRACT"
            column col[:name], col[:type]
          end
          if col[:source].to_sym == :_id
            primary_key [col[:name].to_sym]
          end
        end
      end
    end

    def drop_extracts(sql)
      @map_original.each do |dbname, db|
        db.each do |cname, spec|
          parse_extract(spec[:extract]).each do |x|
            tb = cname.to_s + "." + x
            log.info("Dropping extract #{x} for original table #{@map_original}")
            sql.drop_table?(tb)
          end
        end
      end
    end

    def transform_row(obj, schema, db)
      log.debug("Current Schema #{schema} for obj #{obj}")
      row = []
      if obj.has_key?("_id")
        pid = obj["_id"].to_s
      else
        pid = nil
      end
      schema[:columns].each do |col|
        source = col[:source]
        type = col[:type]
        if col[:type] == "EXTRACT"
          if obj.has_key?(source)
            process_extract(col, obj, pid, db)
          end
          obj.delete(col[:source])
        else
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

      row = transform_row(obj,schema,db)
      if obj.keys.length > 0
        new_columns = get_columns(obj, @map_original[dbname][cname][:columns], @map_original[dbname][cname][:extract])
        @map[dbname][cname][:columns] = @map[dbname][cname][:columns] + new_columns
        row = row + transform_row(obj, {:columns => new_columns }, db)
        alter_schema(schema[:meta][:table], new_columns, db)
      end

      log.debug { "Transformed: #{row.inspect}" }

      row
    end

    def parse_extract(e)
      if e
        return e
      end
      return []
    end

    def parse_columns(c)
      if c
        return c
      end
      return {}
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
              @cname = cname
              @map[dbname][cname] = {:columns => [], :meta => parse_meta(spec[:meta]), :extract => parse_extract(spec[:extract])}
              if @map[dbname][cname][:extract].size() > 0
                checks = []
                @map[dbname][cname][:extract].each do |x|
                  a = {x => { "$exists" => true}, "$where" => "this.#{x}.length>1"}
                  checks << a
                end
                first_item = mongo[dbname][cname].find_one( "$and" => checks )
              else
                first_item = mongo[dbname][cname].find_one()
              end
              if first_item
                @map[dbname][cname][:columns] = get_columns(first_item, map[dbname][cname][:columns], @map[dbname][cname][:extract])
              end
            end
        end
      end
    end
  end
end
