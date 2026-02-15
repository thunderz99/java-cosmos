package io.github.thunderz99.cosmos.util;

import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	 * @param mapper original mapper
	 */
	private static void init(ObjectMapper mapper) {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false) //
                .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL) //
                .setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    }

	/**
	 * Object to json
	 * @param object bean object
	 * @return json string
	 */
	public static String toJson(Object object) {
		try {
			return mapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			processException(object, e);
		}
		return "";
	}

	/**
	 * Object to json without indent
	 * @param object bean object
	 * @return json string
	 */
	public static String toJsonNoIndent(Object object) {
		try {
			return noIndentMapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			processException(object, e);
		}
		return "";
	}

	/**
	 * Object to json using target and mixin
	 * @param object bean object
	 * @param target Class (or interface) whose annotations to effectively override
	 * @param mixinSource Class (or interface) whose
	 * @return json string
	 */
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
	 * @param object bean object
	 * @return map
	 */
	public static Map<String, Object> toMap(Object object) {
		return mapper.convertValue(object, new TypeReference<LinkedHashMap<String, Object>>() {
		});
	}

	/**
	 * Convert to List of Map
	 *
	 * @param json json string
	 * @return List of map
	 */
	public static List<LinkedHashMap<String, Object>> toListOfMap(String json) {
		return fromJson(json, new TypeReference<List<LinkedHashMap<String, Object>>>() {
		});
	}

	/**
	 * Convert to List of Map
	 *
	 * @param is json string inputStream
	 * @return List of map
	 */
	public static List<LinkedHashMap<String, Object>> toListOfMap(InputStream is) {
		return fromJson(is, new TypeReference<List<LinkedHashMap<String, Object>>>() {
		});
	}

	/**
	 * Convert to Map
	 *
	 * @param json json string
	 * @return map
	 */
	public static Map<String, Object> toMap(String json) {
		return fromJson(json, new TypeReference<LinkedHashMap<String, Object>>() {
		});
	}

	/**
	 * Convert to Map. Only a part of json by path.
	 *
	 * @param json json string
	 * @param path part of json. e.g. "/data"
	 * @return map
	 */
	public static Map<String, Object> toMap(String json, String path) {
		return fromJson(json, new TypeReference<LinkedHashMap<String, Object>>() {
		}, path);
	}

	/**
	 * Convert to Map
	 *
	 * @param is json string inputStream
	 * @return map
	 */
	public static Map<String, Object> toMap(InputStream is) {
		return fromJson(is, new TypeReference<LinkedHashMap<String, Object>>() {
		});
	}

	/**
	 * Convert to Map
	 *
	 * @param object bean object
	 * @param target Class (or interface) whose annotations to effectively override
	 * @param mixinSource Class (or interface) whose
	 * @return map
	 */
	public static Map<String, Object> toMap(Object object, Class<?> target, Class<?> mixinSource) {
		return newObjectMapper().addMixIn(target, mixinSource).convertValue(object,
				new TypeReference<LinkedHashMap<String, Object>>() {
				});
	}

	/**
	 * Json string to bean object
	 * @param json json string
	 * @param classOfT class for bean
	 * @param <T> generic for bean
	 * @return bean object
	 */
	public static <T> T fromJson(String json, Class<T> classOfT) {
		try {
			return mapper.readValue(json, classOfT);
		} catch (Exception e) {
			processException(json, e);
		}
		return null;
	}

	/**
	 * Json string to bean object
	 * @param json json string
	 * @param typeRef typeRef for bean
	 * @param <T> generic for bean
	 * @return bean object
	 */
	public static <T> T fromJson(String json, TypeReference<T> typeRef) {
		try {
			return mapper.readValue(json, typeRef);
		} catch (Exception e) {
			processException(json, e);
		}
		return null;
	}

	/**
	 * Json string to bean object
	 * @param json json string
	 * @param className className for bean
	 * @param <T> generic for bean
	 * @return bean object
	 */
	public static <T> T fromJson(String json, String className) {
		try {
			JavaType javaType = constructType(className);
			return mapper.readValue(json, javaType);
		} catch (Exception e) {
			processException(json, e);
		}
		return null;
	}

	/**
	 * Json to List of bean
	 * @param json json string
	 * @param className className for bean
	 * @param <T> generic param for bean
	 * @return List of bean
	 */
	public static <T> List<T> fromJson2List(String json, String className) {
		try {
			JavaType javaType = constructListType(className);
			return mapper.readValue(json, javaType);
		} catch (Exception e) {
			processException(json, e);
		}
		return List.of();
	}

	/**
	 * Json to List of bean
	 * @param json json string
	 * @param classOfT class of bean
	 * @param <T> generic param for bean
	 * @return List of bean
	 */
	public static <T> List<T> fromJson2List(String json, Class<T> classOfT) {
		return fromJson2List(json, classOfT.getName());
	}

	/**
	 * Json string to bean object
	 * @param json json string
	 * @param javaType javaType for bean
	 * @param <T> generic for bean
	 * @return bean object
	 */
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
	 * @param className className to convert
	 * @return javaType
	 * @throws ClassNotFoundException invalid className
	 */
	static JavaType constructType(String className) throws ClassNotFoundException {
		Class<?> clazz = Class.forName(className);
		return mapper.constructType(clazz);
	}

	/**
	 * construct List JavaType from rawType className
	 *
	 * @param className className to convert
	 * @return javaType
	 * @throws ClassNotFoundException invalid className
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
	 * @param json jsonString
	 * @param typeRef typeRef for bean
	 * @param path a part of json
	 * @param <T> generic param for bean
	 * @return bean object
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
	 * @param json json string
	 * @param classOfT class of bean
	 * @param path a part of json
	 * @param <T> generic param for bean
	 * @return bean object
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

	/**
	 * Json string to bean object
	 * @param reader json string reader
	 * @param classOfT class for bean
	 * @param <T> generic for bean
	 * @return bean object
	 */
	public static <T> T fromJson(Reader reader, Class<T> classOfT) {
		try {
			return mapper.readValue(reader, classOfT);
		} catch (Exception e) {
			processException(reader, e);
		}
		return null;
	}

	/**
	 * Json string to bean object
	 * @param is json string inputStream
	 * @param classOfT class for bean
	 * @param <T> generic for bean
	 * @return bean object
	 */
	public static <T> T fromJson(InputStream is, Class<T> classOfT) {
		try {
			return mapper.readValue(is, classOfT);
		} catch (Exception e) {
			processException(is, e);
		}
		return null;
	}

	/**
	 * Json string to bean object
	 * @param is json string inputStream
	 * @param typeRef typeRef for bean
	 * @param <T> generic for bean
	 * @return bean object
	 */
	public static <T> T fromJson(InputStream is, TypeReference<T> typeRef) {
		try {
			return mapper.readValue(is, typeRef);
		} catch (Exception e) {
			processException(is, e);
		}
		return null;
	}

	/**
	 * Json string to bean object
	 * @param is json string inputStream
	 * @param className className for bean
	 * @param <T> generic for bean
	 * @return bean object
	 */
	public static <T> T fromJson(InputStream is, String className) {
		try {
			JavaType javaType = constructType(className);
			return mapper.readValue(is, javaType);
		} catch (Exception e) {
			processException(is, e);
		}
		return null;
	}

    /**
     * Json to List of bean
     *
     * @param is        json string inputStream
     * @param className className for bean
     * @param <T>       generic param for bean
     * @return List of bean
     */
    public static <T> List<T> fromJson2List(InputStream is, String className) {
        try {
            JavaType javaType = constructListType(className);
            return mapper.readValue(is, javaType);
        } catch (Exception e) {
            processException(is, e);
        }
        return List.of();
    }

    /**
     * Json to List of bean
     *
     * @param is       json string inputStream
     * @param classOfT class of bean
     * @param <T>      generic param for bean
     * @return List of bean
     */
    public static <T> List<T> fromJson2List(InputStream is, Class<T> classOfT) {
        try {
            JavaType javaType = constructListType(classOfT.getName());
            return mapper.readValue(is, javaType);
        } catch (Exception e) {
            processException(is, e);
        }
        return List.of();
    }

    /**
     * Json string to bean object
     *
     * @param is       json string inputStream
     * @param javaType javaType for bean
     * @param <T>      generic for bean
     * @return bean object
     */
    public static <T> T fromJson(InputStream is, JavaType javaType) {
        try {
            return mapper.readValue(is, javaType);
        } catch (Exception e) {
            processException(is, e);
        }
        return null;
    }

    /**
     * map to bean object
     *
     * @param map      map representing json
     * @param classOfT class of bean
     * @param <T>      generic param for bean
     * @return bean object
     */
    public static <T> T fromMap(Map<String, Object> map, Class<T> classOfT) {
        try {
            return mapper.convertValue(map, classOfT);
        } catch (Exception e) {
            processException(map, e);
        }
        return null;
    }

    private static void processException(Object object, Exception e) {
        log.warn("json process error. object = {}", object, e);
        String detail = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
        throw new IllegalArgumentException("json process error: " + detail, e);
    }

    static String exceptionToString(Throwable throwable) {
        StringBuilder sb = new StringBuilder();
        sb.append(throwable.getClass().getName());
        if (throwable.getMessage() != null) {
            sb.append(": ").append(throwable.getMessage());
        }
        sb.append("\n");
        for (StackTraceElement stackTraceElement : throwable.getStackTrace()) {
            sb.append("\tat ").append(stackTraceElement.toString()).append("\n");
        }
        return sb.toString();
    }

    private static ObjectMapper newObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        init(mapper);
        return mapper;
    }

    /**
     * return an objectMapper configured the same as this JsonUtil
     *
     * @return objectMapper
     */
    public static ObjectMapper getObjectMapper() {
        return newObjectMapper();
    }

}
