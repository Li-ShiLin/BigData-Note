<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Kafka 公共模块 (kafka-00-common)](#kafka-%E5%85%AC%E5%85%B1%E6%A8%A1%E5%9D%97-kafka-00-common)
  - [项目作用](#%E9%A1%B9%E7%9B%AE%E4%BD%9C%E7%94%A8)
  - [项目结构](#%E9%A1%B9%E7%9B%AE%E7%BB%93%E6%9E%84)
  - [核心功能](#%E6%A0%B8%E5%BF%83%E5%8A%9F%E8%83%BD)
    - [1. 通用工具类 (CommonUtils)](#1-%E9%80%9A%E7%94%A8%E5%B7%A5%E5%85%B7%E7%B1%BB-commonutils)
      - [主要方法](#%E4%B8%BB%E8%A6%81%E6%96%B9%E6%B3%95)
      - [使用示例](#%E4%BD%BF%E7%94%A8%E7%A4%BA%E4%BE%8B)
    - [2. 统一响应结果类 (R)](#2-%E7%BB%9F%E4%B8%80%E5%93%8D%E5%BA%94%E7%BB%93%E6%9E%9C%E7%B1%BB-r)
      - [类结构](#%E7%B1%BB%E7%BB%93%E6%9E%84)
      - [主要方法](#%E4%B8%BB%E8%A6%81%E6%96%B9%E6%B3%95-1)
      - [使用示例](#%E4%BD%BF%E7%94%A8%E7%A4%BA%E4%BE%8B-1)
    - [3. 常量定义 (Constants)](#3-%E5%B8%B8%E9%87%8F%E5%AE%9A%E4%B9%89-constants)
    - [4. 业务异常类 (BusinessException)](#4-%E4%B8%9A%E5%8A%A1%E5%BC%82%E5%B8%B8%E7%B1%BB-businessexception)
      - [构造函数](#%E6%9E%84%E9%80%A0%E5%87%BD%E6%95%B0)
      - [使用示例](#%E4%BD%BF%E7%94%A8%E7%A4%BA%E4%BE%8B-2)
    - [5. 全局异常处理器 (GlobalExceptionHandler)](#5-%E5%85%A8%E5%B1%80%E5%BC%82%E5%B8%B8%E5%A4%84%E7%90%86%E5%99%A8-globalexceptionhandler)
      - [处理的异常类型](#%E5%A4%84%E7%90%86%E7%9A%84%E5%BC%82%E5%B8%B8%E7%B1%BB%E5%9E%8B)
      - [异常处理示例](#%E5%BC%82%E5%B8%B8%E5%A4%84%E7%90%86%E7%A4%BA%E4%BE%8B)
  - [依赖配置](#%E4%BE%9D%E8%B5%96%E9%85%8D%E7%BD%AE)
    - [Maven 依赖](#maven-%E4%BE%9D%E8%B5%96)
  - [在其他项目中使用](#%E5%9C%A8%E5%85%B6%E4%BB%96%E9%A1%B9%E7%9B%AE%E4%B8%AD%E4%BD%BF%E7%94%A8)
    - [1. 添加依赖](#1-%E6%B7%BB%E5%8A%A0%E4%BE%9D%E8%B5%96)
    - [2. 使用工具类](#2-%E4%BD%BF%E7%94%A8%E5%B7%A5%E5%85%B7%E7%B1%BB)
    - [3. 使用统一响应格式](#3-%E4%BD%BF%E7%94%A8%E7%BB%9F%E4%B8%80%E5%93%8D%E5%BA%94%E6%A0%BC%E5%BC%8F)
  - [配置说明](#%E9%85%8D%E7%BD%AE%E8%AF%B4%E6%98%8E)
    - [1. 应用配置](#1-%E5%BA%94%E7%94%A8%E9%85%8D%E7%BD%AE)
    - [2. 全局异常处理配置](#2-%E5%85%A8%E5%B1%80%E5%BC%82%E5%B8%B8%E5%A4%84%E7%90%86%E9%85%8D%E7%BD%AE)
  - [最佳实践](#%E6%9C%80%E4%BD%B3%E5%AE%9E%E8%B7%B5)
    - [1. 异常处理](#1-%E5%BC%82%E5%B8%B8%E5%A4%84%E7%90%86)
    - [2. 响应格式](#2-%E5%93%8D%E5%BA%94%E6%A0%BC%E5%BC%8F)
    - [3. 对象转换](#3-%E5%AF%B9%E8%B1%A1%E8%BD%AC%E6%8D%A2)
  - [注意事项](#%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Kafka 公共模块 (kafka-00-common)

## 项目作用

本项目是 Kafka 系列项目的公共模块，提供了通用的工具类、异常处理、统一响应格式等基础功能，为其他 Kafka 相关项目提供统一的依赖和工具支持。

## 项目结构

```
kafka-00-common/
├── src/main/java/com/action/kafka0common/
│   ├── common/
│   │   ├── CommonUtils.java              # 通用工具类
│   │   ├── Constants.java                # 常量定义
│   │   └── R.java                        # 统一响应结果类
│   ├── exception/
│   │   ├── BusinessException.java        # 业务异常类
│   │   └── GlobalExceptionHandler.java   # 全局异常处理器
│   └── Kafka00CommonApplication.java     # 主启动类
├── src/main/resources/
│   └── application.properties            # 配置文件
└── pom.xml                              # Maven配置
```

## 核心功能

### 1. 通用工具类 (CommonUtils)

提供 JSON 序列化和反序列化的通用方法，基于 FastJSON2 实现。

#### 主要方法

```java
/**
 * 将 JSON 字符串转换为指定类型的对象
 *
 * @param object JSON 字符串
 * @param c 目标类型
 * @return 转换后的对象，如果输入为空则返回 null
 */
public static <T> T convertString(String object, Class<T> c)

/**
 * 将任意对象转换为指定类型的对象
 *
 * @param object 源对象
 * @param c 目标类型
 * @return 转换后的对象，如果输入为 null 则返回 null
 */
public static <T> T convert(Object object, Class<T> c)

/**
 * 将任意对象转换为指定类型的对象列表
 *
 * @param object 源对象
 * @param c 目标类型
 * @return 转换后的对象列表
 */
public static <T> List<T> convertList(Object object, Class<T> c)
```

#### 使用示例

```java
// 将 JSON 字符串转换为 User 对象
String userJson = "{\"id\":1,\"name\":\"张三\",\"age\":25}";
User user = CommonUtils.convertString(userJson, User.class);

// 将 User 对象转换为 JSON 字符串
User user = new User(1, "张三", 25);
String userJson = CommonUtils.convert(user, String.class);

// 将对象列表转换为指定类型列表
List<Map<String, Object>> mapList = Arrays.asList(
        Map.of("id", 1, "name", "张三"),
        Map.of("id", 2, "name", "李四")
);
List<User> userList = CommonUtils.convertList(mapList, User.class);
```

### 2. 统一响应结果类 (R)

提供统一的 API 响应格式，包含状态码、消息和数据。

#### 类结构

```java
public class R<T> implements Serializable {
    private String status;  // 状态码
    private String msg;     // 消息
    private T data;         // 数据
}
```

#### 主要方法

```java
// 成功响应（无数据）
public static <T> R<T> success()

// 成功响应（带数据）
public static <T> R<T> success(T data)

// 失败响应
public static <T> R<T> fail(String code, String msg)
```

#### 使用示例

```java
// 成功响应
return R.success();
return R.

success(userData);

// 失败响应
return R.

fail("1001","用户不存在");
return R.

fail(Constants.ERROR_CODE, "系统异常");
```

### 3. 常量定义 (Constants)

定义系统中使用的通用常量。

```java
public class Constants {
    /**
     * 错误码
     */
    public static final String ERROR_CODE = "999999";

    /**
     * 成功码
     */
    public static final String SUCCESS_CODE = "000000";
}
```

### 4. 业务异常类 (BusinessException)

自定义业务异常，支持错误码和异常信息。

#### 构造函数

```java
// 使用默认错误码
public BusinessException(String message)

// 指定错误码和消息
public BusinessException(String code, String message)

// 带异常原因
public BusinessException(String message, Throwable cause)

public BusinessException(String code, String message, Throwable cause)
```

#### 使用示例

```java
// 抛出业务异常
throw new BusinessException("用户不存在");
throw new

BusinessException("1001","用户不存在");
throw new

BusinessException("1001","用户不存在",cause);
```

### 5. 全局异常处理器 (GlobalExceptionHandler)

统一处理系统中的各种异常，提供友好的错误响应。

#### 处理的异常类型

1. **参数校验异常** (`MethodArgumentNotValidException`)
    - 处理 `@Valid` 注解的参数校验失败

2. **缺少参数异常** (`MissingServletRequestParameterException`)
    - 处理缺少必需请求参数的情况

3. **约束违反异常** (`ConstraintViolationException`)
    - 处理 `@Validated` 注解的约束校验失败

4. **业务异常** (`BusinessException`)
    - 处理自定义的业务异常

5. **系统异常** (`Exception`)
    - 处理其他未捕获的异常

#### 异常处理示例

```java

@RestController
public class UserController {

    @PostMapping("/user")
    public R<User> createUser(@Valid @RequestBody User user) {
        // 如果 user 对象校验失败，会被 GlobalExceptionHandler 捕获
        // 返回格式：{"status":"999999","msg":"[字段校验错误信息]","data":null}
        return R.success(userService.create(user));
    }

    @GetMapping("/user/{id}")
    public R<User> getUser(@PathVariable Long id) {
        if (id <= 0) {
            throw new BusinessException("1001", "用户ID必须大于0");
        }
        // 业务异常会被 GlobalExceptionHandler 捕获
        // 返回格式：{"status":"1001","msg":"用户ID必须大于0","data":null}
        return R.success(userService.findById(id));
    }
}
```

## 依赖配置

### Maven 依赖

```xml

<dependencies>
    <!-- Spring Boot 核心依赖 -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter</artifactId>
    </dependency>

    <!-- Spring Boot Web 依赖 -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- Jakarta Servlet API (Spring Boot 3.x) -->
    <dependency>
        <groupId>jakarta.servlet</groupId>
        <artifactId>jakarta.servlet-api</artifactId>
        <scope>provided</scope>
    </dependency>

    <!-- 参数校验依赖 -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-validation</artifactId>
    </dependency>

    <!-- Lombok 代码生成 -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>

    <!-- FastJSON2 JSON 处理 -->
    <dependency>
        <groupId>com.alibaba.fastjson2</groupId>
        <artifactId>fastjson2</artifactId>
    </dependency>

    <!-- Spring Boot 测试依赖 -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## 在其他项目中使用

### 1. 添加依赖

在需要使用公共模块的项目 `pom.xml` 中添加依赖：

```xml

<dependencies>
    <!-- 公共模块依赖 -->
    <dependency>
        <groupId>com.action</groupId>
        <artifactId>kafka-00-common</artifactId>
        <version>0.0.1-SNAPSHOT</version>
    </dependency>

    <!-- 其他依赖... -->
</dependencies>
```

### 2. 使用工具类

```java
import com.action.kafka0common.common.CommonUtils;
import com.action.kafka0common.common.R;
import com.action.kafka0common.exception.BusinessException;

@Service
public class UserService {

    public R<User> createUser(User user) {
        try {
            // 使用 CommonUtils 进行对象转换
            String userJson = CommonUtils.convert(user, String.class);
            log.info("创建用户: {}", userJson);

            // 业务逻辑处理
            User savedUser = userRepository.save(user);

            return R.success(savedUser);
        } catch (Exception e) {
            throw new BusinessException("1001", "创建用户失败: " + e.getMessage());
        }
    }
}
```

### 3. 使用统一响应格式

```java

@RestController
@RequestMapping("/api/users")
public class UserController {

    @GetMapping("/{id}")
    public R<User> getUser(@PathVariable Long id) {
        User user = userService.findById(id);
        if (user == null) {
            throw new BusinessException("1002", "用户不存在");
        }
        return R.success(user);
    }

    @PostMapping
    public R<User> createUser(@Valid @RequestBody User user) {
        User savedUser = userService.create(user);
        return R.success(savedUser);
    }
}
```

## 配置说明

### 1. 应用配置

```properties
# 应用名称
spring.application.name=kafka-00-common
# 服务器端口
server.port=8080
# 日志配置
logging.level.com.action.kafka0common=DEBUG
logging.pattern.console=%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n
```

### 2. 全局异常处理配置

全局异常处理器会自动生效，无需额外配置。支持的配置项：

- 日志级别：通过 `logging.level.com.action.kafka0common.exception` 控制异常日志级别
- 异常信息：异常处理器会记录详细的请求信息和异常堆栈

## 最佳实践

### 1. 异常处理

```java
// 推荐：使用业务异常
if(user ==null){
        throw new

BusinessException("1001","用户不存在");
}

// 推荐：使用带错误码的异常
        throw new

BusinessException("1002","用户已存在",cause);

// 不推荐：直接抛出 RuntimeException
throw new

RuntimeException("用户不存在");
```

### 2. 响应格式

```java
// 推荐：使用统一响应格式
@GetMapping("/user/{id}")
public R<User> getUser(@PathVariable Long id) {
    User user = userService.findById(id);
    return R.success(user);
}

// 不推荐：直接返回对象
@GetMapping("/user/{id}")
public User getUser(@PathVariable Long id) {
    return userService.findById(id);
}
```

### 3. 对象转换

```java
// 推荐：使用 CommonUtils 进行类型转换
String json = CommonUtils.convert(user, String.class);
User user = CommonUtils.convertString(json, User.class);

// 不推荐：直接使用 FastJSON2
String json = JSON.toJSONString(user);
User user = JSON.parseObject(json, User.class);
```

## 注意事项

1. **版本兼容性**：本模块基于 Spring Boot 3.x 和 Jakarta EE，不兼容 Spring Boot 2.x
2. **异常处理**：全局异常处理器会自动捕获所有异常，确保 API 返回统一格式
3. **JSON 处理**：使用 FastJSON2 进行 JSON 序列化和反序列化，性能优于 Jackson
4. **参数校验**：支持 JSR-303 参数校验注解，如 `@Valid`、`@NotNull` 等
5. **日志记录**：异常处理器会记录详细的请求信息和异常堆栈，便于问题排查
6. **线程安全**：所有工具类方法都是线程安全的，可以在多线程环境中使用