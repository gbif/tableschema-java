package io.frictionlessdata.tableschema.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.frictionlessdata.tableschema.exception.JsonParsingException;
import io.frictionlessdata.tableschema.exception.JsonSerializingException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public final class JsonUtil {
	private static JsonUtil instance;
	private ObjectMapper mapper;
	
	private JsonUtil() {
		this.mapper = JsonMapper.builder()
			.enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
			.enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
			.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
			.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
			.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
			.findAndAddModules()
			.build()
			.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			//.setDefaultSetterInfo(JsonSetter.Value.forContentNulls(Nulls.AS_EMPTY));;
	}
	
	public static JsonUtil getInstance() {
		if (Objects.isNull(instance)) {
			instance = new JsonUtil();
		}
		return instance;
	}

	public ObjectMapper getMapper(){return mapper;}


	public ObjectNode createNode() {
		return mapper.createObjectNode();
	}
	
	public TextNode createTextNode(String value) {
		return new TextNode(value);
	}
	
	public JsonNode createNode(String content) {
		try {
			return mapper.readTree(content);
		} catch (JsonProcessingException e) {
			throw new JsonParsingException(e);
		}
	}
	
	public JsonNode createNode(Object content) {
		try {
			String json = mapper.writeValueAsString(content);
			try {
				return mapper.readTree(json);
			} catch (JsonMappingException e) {
				throw new JsonParsingException(e);
			} 
		} catch (JsonProcessingException e) {
			throw new JsonSerializingException(e);
		}
	}
	
	public ArrayNode createArrayNode() {
		return mapper.createArrayNode();
	}
	
	public ArrayNode createArrayNode(String content) {
		try {
			return (ArrayNode)mapper.readTree(content);
		} catch (JsonProcessingException e) {
			throw new JsonParsingException(e);
		}
	}
	
	public ArrayNode createArrayNode(Object content) {
		return (ArrayNode) createNode(content);
	}

	public String serialize(Object value) {
		return serialize (value, true);
	}
	public String serialize(Object value, boolean indent) {
		try {
			return _getWriter(indent).writeValueAsString(value);
		} catch (JsonProcessingException e) {
			throw new JsonSerializingException(e);
		}
	}

	public <T> T deserialize(String value, Class<T> clazz) {
		try {
			return mapper.readValue(sanitize(value), clazz);
		} catch (JsonProcessingException e) {
			throw new JsonParsingException(e);
		}
	}
	
	public <T> T deserialize(String value, TypeReference<T> typeRef) {
		try {
			return mapper.readValue(sanitize(value), typeRef);
		} catch (JsonProcessingException e) {
			throw new JsonParsingException(e);
		}
	}

	public <T> T deserialize(JsonNode value, TypeReference<T> typeRef) {
		return mapper.convertValue(value, typeRef);
	}

	public <T> T deserialize(JsonNode value, Class<T> clazz) {
		return mapper.convertValue(value, clazz);
	}

	public JsonNode readValue(String value) {
		try {
			return mapper.readTree(sanitize(value));
		} catch (JsonProcessingException e) {
			throw new JsonParsingException(e);
		}
	}
	
	public JsonNode readValue(InputStream value) {
		try {
			return mapper.readTree(value);
		} catch (IOException e) {
			throw new JsonParsingException(e);
		}
	}

	public <T> T convertValue(Object value, TypeReference<T> ref) {
		return mapper.convertValue(value, ref);
	}
	
	public <T> T convertValue(Object value, Class<T> clazz) {
		return mapper.convertValue(value, clazz);
	}
	
	// if it uses the extended double quote character sometimes found in CSV files
	private String sanitize(String string) {
		if(string.startsWith("[“") || string.startsWith("{“")) {
    		// replace both left and right versions
    		return string.replace("“", "\"").replace("”", "\"");
    	} else return string;
	}

	private ObjectWriter _getWriter(boolean indent) {
		return (indent) ? mapper.writer(new DefaultPrettyPrinter()) : mapper.writer(new MinimalPrettyPrinter());
	}
	
}
