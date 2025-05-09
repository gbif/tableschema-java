package io.frictionlessdata.tableschema.schema;

import io.frictionlessdata.tableschema.exception.TypeInferringException;
import io.frictionlessdata.tableschema.field.Field;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static io.frictionlessdata.tableschema.field.Field.FIELD_TYPE_ANY;


/**
 * The type inferral algorithm tries to cast to available types and each successful
 * type casting increments a popularity score for the successful type cast in question.
 * At the end, the best score so far is returned.
 */
public class TypeInferrer {
    
    /**
     * We are using reflection to go through the cast methods
     * so we want to make this a Singleton to avoid instanciating
     * a new class for every cast method call attempt.
     */
    private static TypeInferrer instance = null;

    private final Map<String, Map<String, Integer>> typeInferralMap = new HashMap<>();
    private final Map<String, String> formatMap = new HashMap<>();
    
    // The order in which the types will be attempted to be inferred.
    // Once a type is successfully inferred, we do not bother with the remaining types.
    private static final List<String[]> TYPE_INFERRAL_ORDER_LIST = new ArrayList<>(Arrays.asList(
        new String[]{Field.FIELD_TYPE_GEOPOINT, Field.FIELD_FORMAT_DEFAULT},
        new String[]{Field.FIELD_TYPE_GEOPOINT, Field.FIELD_FORMAT_ARRAY},
        new String[]{Field.FIELD_TYPE_GEOPOINT, Field.FIELD_FORMAT_OBJECT},
        new String[]{Field.FIELD_TYPE_DURATION, Field.FIELD_FORMAT_DEFAULT}, // No different formats, just use default.
        new String[]{Field.FIELD_TYPE_YEAR, Field.FIELD_FORMAT_DEFAULT}, // No different formats, just use default.
        new String[]{Field.FIELD_TYPE_YEARMONTH, Field.FIELD_FORMAT_DEFAULT}, // No different formats, just use default.
        new String[]{Field.FIELD_TYPE_DATE, Field.FIELD_FORMAT_DEFAULT}, // No different formats, just use default.
        new String[]{Field.FIELD_TYPE_TIME, Field.FIELD_FORMAT_DEFAULT}, // No different formats, just use default.
        new String[]{Field.FIELD_TYPE_DATETIME, Field.FIELD_FORMAT_DEFAULT}, // No different formats, just use default.
        new String[]{Field.FIELD_TYPE_INTEGER, Field.FIELD_FORMAT_DEFAULT}, // No different formats, just use default.
        new String[]{Field.FIELD_TYPE_NUMBER, Field.FIELD_FORMAT_DEFAULT}, // No different formats, just use default.
        new String[]{Field.FIELD_TYPE_BOOLEAN, Field.FIELD_FORMAT_DEFAULT}, // No different formats, just use default.
        new String[]{Field.FIELD_TYPE_GEOJSON, Field.FIELD_FORMAT_DEFAULT},
        new String[]{Field.FIELD_TYPE_GEOJSON, Field.FIELD_FORMAT_TOPOJSON},
        new String[]{Field.FIELD_TYPE_OBJECT, Field.FIELD_FORMAT_DEFAULT},
        new String[]{Field.FIELD_TYPE_ARRAY, Field.FIELD_FORMAT_DEFAULT},
        new String[]{Field.FIELD_TYPE_STRING, Field.FIELD_FORMAT_DEFAULT}, // No different formats, just use default.
        new String[]{FIELD_TYPE_ANY, Field.FIELD_FORMAT_DEFAULT})); // No different formats, just use default.

    
    private TypeInferrer(){
        // Private to enforce use of Singleton pattern.
    }
    
    static TypeInferrer getInstance() {
      if(instance == null) {
         instance = new TypeInferrer();
      }
      return instance;
   }
    
    /**
     * Infer the data types and return the generated schema. Do not limit the
     * number of rows to scan - this can take a long time depending on your
     * data size
     * @param data List of table rows
     * @param headers the table headers
     * @return Return Schema as a String
     * @throws TypeInferringException if inferring the schema fails
     */
    synchronized String infer(List<Object[]> data, String[] headers) throws TypeInferringException{
        return this.infer(data, headers, -1);
    }
    
    /**
     * Infer the data types and return the generated schema.
     * @param data  List of table rows
     * @param headers the table headers
     * @param rowLimit the max number of rows to scan
     * @return Return Schema as a String
     * @throws TypeInferringException  if inferring the schema fails
     */
    synchronized String infer(List<Object[]> data, String[] headers, int rowLimit) throws TypeInferringException{
        // If the given row limit is bigger than the length of the data
        // then just use the length of the data.
        if(rowLimit > data.size()-1){
            rowLimit = data.size()-1;
            //If `rowLimit ` is '-1', no row limiting will be enforced
        } else if(rowLimit == -1){
            rowLimit = data.size()-1;
        }


        // The array that will define the fields in the schema JSON Object.
        List<Map<String,Object>> fieldArray = new ArrayList<>();
        
        // Init the type inferral map and init the schema objects
        for (String header : headers) {
            // Init the type inferral map to track our inferences for each row.
            this.getTypeInferralMap().put(header, new HashMap<>());
            
            // Init the schema objects
            Map<String, Object> fieldObj = new HashMap<>();
            fieldObj.put(Field.JSON_KEY_NAME, header);
            fieldObj.put(Field.JSON_KEY_TITLE, ""); // This will stay blank.
            fieldObj.put(Field.JSON_KEY_DESCRIPTION, ""); // This will stay blank.
            fieldObj.put(Field.JSON_KEY_FORMAT, ""); // This will bet set post inferral.
            fieldObj.put(Field.JSON_KEY_TYPE, ""); // This will bet set post inferral.

            // Wrap it all in an array.
            fieldArray.add(fieldObj);
        }

        // Find the type for each column data for each row.
        // This uses method invokation via reflection in a foor loop that iterates
        // for each possible type/format combo. Insprect the findType method for implementation.
        for(int i = 0; i <= rowLimit; i++){
            Object[] row = data.get(i);
            
            for(int j = 0; j < row.length; j++){
                this.findType(headers[j], (null == row[j]) ? "" : row[j].toString());
            }
        }
        
        // We are done inferring types.
        // Now for each field we figure out which type was the most inferred and settle for that type
        // as the final type for the field.
        for(int j=0; j < fieldArray.size(); j++){
            String fieldName = fieldArray.get(j).get(Field.JSON_KEY_NAME).toString();
            Map<String, Integer> typeInferralCountMap = this.getTypeInferralMap().get(fieldName);
            NavigableMap<String, Integer> typeInferralCountMapSortedByCount = sortMapByValue(typeInferralCountMap);
           
            if(!typeInferralCountMapSortedByCount.isEmpty()){
                String inferredType = typeInferralCountMapSortedByCount.firstEntry().getKey();
                fieldArray.get(j).put(Field.JSON_KEY_TYPE, inferredType);
                fieldArray.get(j).put(Field.JSON_KEY_FORMAT, formatMap.get(headers[j]));
            }
        }
        
        // Need to clear the inferral map for the next inferral call:
        this.getTypeInferralMap().clear();
        this.formatMap.clear();

        // Now that the types have been inferred and set, we build and return the schema object.
        Map<String, Object> schemaJsonObject = new HashMap<>();
        schemaJsonObject.put(Schema.JSON_KEY_FIELDS, fieldArray);
        
        return JsonUtil.getInstance().serialize(schemaJsonObject);
    }
    
    private void findType(String header, String datum){
        // fixes https://github.com/frictionlessdata/tableschema-java/issues/72
        if (StringUtils.isEmpty(datum)) {
            updateInferralScoreMap(header, FIELD_TYPE_ANY);
            formatMap.put(header, "default");
            return;
        }
        // Go through all the field types and call their parsing method to find
        // the first that won't throw
        for(String[] typeInferralDefinition: TYPE_INFERRAL_ORDER_LIST){
            // Keep invoking the type casting methods until one doesn't throw an exception
            String dataType = typeInferralDefinition[0];

            Field<?> field = Field.forType(dataType);
            String format = formatMap.get(header);
            if (null == format) {
                format = field.parseFormat(datum, null);
            }
            if (field.isCompatibleValue(datum, format)) {
                this.formatMap.put(header, format);
                // If no exception is thrown, it means that a type has been inferred.
                // Let's keep track of it in the inferral map.
                this.updateInferralScoreMap(header, field.getType());

                // We no longer need to try to infer other types.
                // Let's break out of the loop.
                break;
            }
        }
    }
    
    /**
     * The type inferral map is where we keep track of the types inferred for values within the same field.
     * @param header the column header in the table
     * @param typeKey type of the column
     */
    private void updateInferralScoreMap(String header, String typeKey){
        if(this.getTypeInferralMap().get(header).containsKey(typeKey)){
            int newCount = this.typeInferralMap.get(header).get(typeKey) + 1;
            this.getTypeInferralMap().get(header).replace(typeKey, newCount);
        }else{
            this.getTypeInferralMap().get(header).put(typeKey, 1);
        }
    }
    
    /**
     * We use a map to keep track the inferred type counts for each field.
     * Once we are done inferring, we settle for the type with that was inferred
     * the most for the same field. Here we sort by score
     * @param map the mapping of types to score to sort
     * @return sorted mapping of types to score
     */
    private NavigableMap<String, Integer> sortMapByValue(Map<String, Integer> map){
        Comparator<String> comparator = new MapValueComparator(map);
        NavigableMap<String, Integer> result = new TreeMap<>(comparator);
        result.putAll(map);

        return result;
    }

    private Map<String, Map<String, Integer>> getTypeInferralMap(){
        return this.typeInferralMap;
    }
}