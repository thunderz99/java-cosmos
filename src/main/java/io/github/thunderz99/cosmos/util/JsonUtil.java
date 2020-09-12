package io.github.thunderz99.cosmos.util;

import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Simple Json Converter based on jackson. Do common initialization and wrap
 * some method for simple use.
 *
 * @author thunderz99
 *
 */
public class JsonUtil {

	private static ObjectMapper mapper = new ObjectMapper();
	private static ObjectMapper noIndentMapper = new ObjectMapper();

	JsonUtil() {
	}

	static {
		init(mapper);
		init(noIndentMapper);

		mapper.enable(SerializationFeature.INDENT_OUTPUT);
	}

	private static final Logger log = LoggerFactory.getLogger(JsonUtil.class);

	/**
	 * common initialization
	 *
	 * @param mapper
	 */
	private static void init(ObjectMapper mapper) {
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false) //
				.setSerializationInclusion(JsonInclude.Include.NON_NULL) //
				.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
	}

	public static String toJson(Object object) {
		try {
			return mapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			processException(object, e);
		}
		return "";
	}

	public static String toJsonNoIndent(Object object) {
		try {
			return noIndentMapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			processException(object, e);
		}
		return "";
	}

	public static String toJson(Object object, Class<?> target, Class<?> mixinSource) {
		try {
			return newObjectMapper().addMixIn(target, mixinSource).writeValueAsString(object);
		} catch (JsonProcessingException e) {
			processException(object, e);
		}
		return "";
	}

	/**
	 * Convert to Map
	 *
	 * @param object
	 * @return
	 */
	public static Map<String, Object> toMap(Object object) {
		return mapper.convertValue(object, new TypeReference<LinkedHashMap<String, Object>>() {
		});
	}

	/**
	 * Convert to Map
	 *
	 * @param object
	 * @return
	 */
	public static Map<String, Object> toMap(String json) {
		return fromJson(json, new TypeReference<LinkedHashMap<String, Object>>() {
		});
	}

	/**
	 * Convert to Map. Only a part of json by path.
	 *
	 * @param object
	 * @return
	 */
	public static Map<String, Object> toMap(String json, String path) {
		return fromJson(json, new TypeReference<LinkedHashMap<String, Object>>() {
		}, path);
	}

	/**
	 * Convert to Map
	 *
	 * @param is
	 * @return
	 */
	public static Map<String, Object> toMap(InputStream is) {
		return fromJson(is, new TypeReference<LinkedHashMap<String, Object>>() {
		});
	}

	/**
	 * Convert to Map
	 *
	 * @param object
	 * @param target
	 * @param mixinSource
	 * @return
	 */
	public static Map<String, Object> toMap(Object object, Class<?> target, Class<?> mixinSource) {
		return newObjectMapper().addMixIn(target, mixinSource).convertValue(object,
				new TypeReference<LinkedHashMap<String, Object>>() {
				});
	}

	public static <T> T fromJson(String json, Class<T> classOfT) {
		try {
			return mapper.readValue(json, classOfT);
		} catch (Exception e) {
			processException(json, e);
		}
		return null;
	}

	public static <T> T fromJson(String json, TypeReference<T> typeRef) {
		try {
			return mapper.readValue(json, typeRef);
		} catch (Exception e) {
			processException(json, e);
		}
		return null;
	}

	public static <T> T fromJson(String json, String className) {
		try {
			JavaType javaType = constructType(className);
			return mapper.readValue(json, javaType);
		} catch (Exception e) {
			processException(json, e);
		}
		return null;
	}

	public static <T> List<T> fromJson2List(String json, String className) {
		try {
			JavaType javaType = constructListType(className);
			return mapper.readValue(json, javaType);
		} catch (Exception e) {
			processException(json, e);
		}
		return List.of();
	}

	public static <T> List<T> fromJson2List(String json, Class<T> classOfT) {
		return fromJson2List(json, classOfT.getName());
	}

	public static <T> T fromJson(String json, JavaType javaType) {
		try {
			return mapper.readValue(json, javaType);
		} catch (Exception e) {
			processException(json, e);
		}
		return null;
	}

	/**
	 * construct JavaType from raw className
	 *
	 * @param clazz
	 * @return
	 * @throws ClassNotFoundException
	 */
	static JavaType constructType(String className) throws ClassNotFoundException {
		Class<?> clazz = Class.forName(className);
		return mapper.constructType(clazz);
	}

	/**
	 * construct List JavaType from rawType className
	 *
	 * @param clazz
	 * @return
	 * @throws ClassNotFoundException
	 */
	static JavaType constructListType(String className) throws ClassNotFoundException {
		JavaType javaType = constructType(className);
		return mapper.getTypeFactory().constructCollectionType(ArrayList.class, javaType);
	}

	/**
	 * Convert to Object using only part of json.
	 * <p>
	 * path'sample： "/tagGroups/tags/name"
	 * </p>
	 *
	 * @param json
	 * @param typeRef
	 * @param path
	 * @return
	 */
	public static <T> T fromJson(String json, TypeReference<T> typeRef, String path) {
		try {
			JsonNode root = mapper.readTree(json);
			String subJson = root.at(path).toString();
			return mapper.readValue(subJson, typeRef);
		} catch (Exception e) {
			processException(json, e);
		}
		return null;
	}

	/**
	 * Convert to Object using only part of json.
	 * <p>
	 * path'sample： "/tagGroups/tags/name"
	 * </p>
	 *
	 * @param json
	 * @param classOfT
	 * @param path
	 * @return
	 */
	public static <T> T fromJson(String json, Class<T> classOfT, String path) {
		try {
			JsonNode root = mapper.readTree(json);
			String subJson = root.at(path).toString();
			return mapper.readValue(subJson, classOfT);
		} catch (Exception e) {
			processException(json, e);
		}
		return null;
	}

	public static <T> T fromJson(Reader reader, Class<T> classOfT) {
		try {
			return mapper.readValue(reader, classOfT);
		} catch (Exception e) {
			processException(reader, e);
		}
		return null;
	}

	public static <T> T fromJson(InputStream is, Class<T> classOfT) {
		try {
			return mapper.readValue(is, classOfT);
		} catch (Exception e) {
			processException(is, e);
		}
		return null;
	}

	public static <T> T fromJson(InputStream is, TypeReference<T> typeRef) {
		try {
			return mapper.readValue(is, typeRef);
		} catch (Exception e) {
			processException(is, e);
		}
		return null;
	}

	public static <T> T fromJson(InputStream is, String className) {
		try {
			JavaType javaType = constructType(className);
			return mapper.readValue(is, javaType);
		} catch (Exception e) {
			processException(is, e);
		}
		return null;
	}

	public static <T> List<T> fromJson2List(InputStream is, String className) {
		try {
			JavaType javaType = constructListType(className);
			return mapper.readValue(is, javaType);
		} catch (Exception e) {
			processException(is, e);
		}
		return List.of();
	}

	public static <T> List<T> fromJson2List(InputStream is, Class<T> classOfT) {
		try {
			JavaType javaType = constructListType(classOfT.getName());
			return mapper.readValue(is, javaType);
		} catch (Exception e) {
			processException(is, e);
		}
		return List.of();
	}

	public static <T> T fromJson(InputStream is, JavaType javaType) {
		try {
			return mapper.readValue(is, javaType);
		} catch (Exception e) {
			processException(is, e);
		}
		return null;
	}

	public static <T> T fromMap(Map<String, Object> map, Class<T> classOfT) {
		try {
			return mapper.convertValue(map, classOfT);
		} catch (Exception e) {
			processException(map, e);
		}
		return null;
	}

	private static void processException(Object object, Exception e) {
		log.error("json error: ", e);
		throw new IllegalArgumentException("json process error.", e);
	}

	private static ObjectMapper newObjectMapper() {
		ObjectMapper mapper = new ObjectMapper();
		init(mapper);
		return mapper;
	}

	public static ObjectMapper getObjectMapper() {
		return newObjectMapper();
	}

}