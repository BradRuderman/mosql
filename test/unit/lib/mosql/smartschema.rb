require File.join(File.dirname(__FILE__), '../../../_lib.rb')
require 'json'

class MoSQL::Test::SmartSchemaTest < MoSQL::Test
  TEST_MAP = <<EOF
---
mydb:
  testData:
    :columns:
      - _id: TEXT
      - x: INTEGER
      - j: INTEGER
    :meta:
     :table: things
     :extra_props: true
EOF
  before do
    @map = MoSQL::SmartSchema
  end
  it 'properly recurses objects' do
    value = %{
{
  \"objField\": {
    \"objNestedField\" : \"brad\",
    \"objNestedField2\" : \"Ruderman\",
    \"objNestedObj\" : {
      \"objNestedObj\" : {
        \"objNestedObj\" : {
          \"testStr\" : \"abcdefghijkl\"
        }
      }
    }
  },
  \"intField\":5515,
  \"boolField\":false,
  \"arrayIntField\" : [1,2,3],
  \"arrayObjField\" : [
    {
      \"strField\" : \"nested\"
    },
    {
      \"strField\" : \"nested2\"
    }
  ]
}
}
    hash = JSON.parse(value)
    vals = @map.recurse_obj("objField", hash["objField"])
    correct_array = [
      { :source => "objField.objNestedField", :name => "objField_objNestedField", :type => "TEXT"},
      { :source => "objField.objNestedField2", :name => "objField_objNestedField2", :type => "TEXT"},
      { :source => "objField.objNestedObj.objNestedObj.objNestedObj.testStr", :name => "objField_objNestedObj_objNestedObj_objNestedObj_testStr", :type => "TEXT"}
    ]
    assert_equal(correct_array,vals)
  end
end
