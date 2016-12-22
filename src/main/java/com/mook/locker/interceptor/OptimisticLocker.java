/**
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016 342252328@qq.com
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.mook.locker.interceptor;

import com.mook.locker.annotation.VersionLocker;
import com.mook.locker.cache.Cache;
import com.mook.locker.cache.Cache.MethodSignature;
import com.mook.locker.cache.LocalVersionLockerCache;
import com.mook.locker.cache.LockerSession;
import com.mook.locker.cache.VersionLockerCache;
import com.mook.locker.util.PluginUtil;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.ibatis.binding.BindingException;
import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeException;
import org.apache.ibatis.type.TypeHandler;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * <p>MyBatis乐观锁插件<br>
 * <p>MyBatis Optimistic Locker Plugin<br>
 *
 * @author 342252328@qq.com
 * @version 1.0
 * @date 2016-05-27
 * @since JDK1.7
 */
@Intercepts({
        @Signature(type = StatementHandler.class, method = "prepare", args = {Connection.class, Integer.class}),
        @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }),
        @Signature(type = ParameterHandler.class, method = "setParameters", args = {PreparedStatement.class})
})
public class OptimisticLocker implements Interceptor {

    private static final Log log = LogFactory.getLog(OptimisticLocker.class);

    private static VersionLocker trueLocker;

    static {
        try {
            trueLocker = OptimisticLocker.class.getDeclaredMethod("versionValue").getAnnotation(VersionLocker.class);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("The plugin init faild." + e, e);
        }
    }

    private Properties props = null;
    private VersionLockerCache versionLockerCache = new LocalVersionLockerCache();
    Map<String, Class<?>> mapperMap = null;

    @VersionLocker(true)
    private void versionValue() {
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object intercept(Invocation invocation) throws Exception {
        String versionField;
        String versionColumn;
        if (null == props || props.isEmpty()) {
            versionColumn = "version";
            versionField = "version";
        } else {
            versionColumn = props.getProperty("versionColumn", "version");
            versionField = props.getProperty("versionField", "version");
        }

        String interceptMethod = invocation.getMethod().getName();
        if ("prepare".equals(interceptMethod)) {

            StatementHandler handler = (StatementHandler) PluginUtil.processTarget(invocation.getTarget());
            MetaObject hm = SystemMetaObject.forObject(handler);

            MappedStatement ms = (MappedStatement) hm.getValue("delegate.mappedStatement");
            SqlCommandType sqlCmdType = ms.getSqlCommandType();

            if (sqlCmdType != SqlCommandType.UPDATE ) {
                return invocation.proceed();
            }

            BoundSql boundSql = (BoundSql) hm.getValue("delegate.boundSql");

            VersionLocker vl = getVersionLocker(ms, boundSql);
            if (null != vl && !vl.value()) {
                return invocation.proceed();
            }

            Object originalVersion = hm.getValue("delegate.boundSql.parameterObject." + versionField);
            if (originalVersion == null || Long.parseLong(originalVersion.toString()) < 0) {
                throw new BindingException("value of version field[" + versionField + "]can not be empty or less than zero");
            }

            Object versionIncr = castTypeAndOptValue(originalVersion, hm.getValue("delegate.boundSql.parameterObject"), ValueType.INCREASE);
            hm.setValue("delegate.boundSql.parameterObject." + versionColumn, versionIncr);

            String originalSql = boundSql.getSql();
            if (log.isDebugEnabled()) {
                log.debug("==> originalSql: " + originalSql);
            }

            originalSql = addVersionToSql(originalSql, versionColumn, originalVersion);
            hm.setValue("delegate.boundSql.sql", originalSql);
            if (log.isDebugEnabled()) {
                log.debug("==> originalSql after add version: " + originalSql);
            }

            LockerSession.setLockerFlag(true);

            return invocation.proceed();

        } else if ("setParameters".equals(interceptMethod)) {

            ParameterHandler handler = (ParameterHandler) PluginUtil.processTarget(invocation.getTarget());
            MetaObject hm = SystemMetaObject.forObject(handler);

            MappedStatement ms = (MappedStatement) hm.getValue("mappedStatement");
            SqlCommandType sqlCmdType = ms.getSqlCommandType();
            if (sqlCmdType != SqlCommandType.UPDATE) {
                return invocation.proceed();
            }

            Configuration configuration = ms.getConfiguration();
            BoundSql boundSql = (BoundSql) hm.getValue("boundSql");

            VersionLocker vl = getVersionLocker(ms, boundSql);
            if (null != vl && !vl.value()) {
                return invocation.proceed();
            }

            Object result = invocation.proceed();

            ParameterMapping versionMapping = new ParameterMapping.Builder(configuration, versionField, Object.class).build();

            Object parameterObject = boundSql.getParameterObject();

            MetaObject pm = configuration.newMetaObject(parameterObject);
            if (parameterObject instanceof MapperMethod.ParamMap<?>) {
                MapperMethod.ParamMap<?> paramMap = (MapperMethod.ParamMap<?>) parameterObject;
                if (!paramMap.containsKey(versionField)) {
                    throw new TypeException("All the primitive type parameters must add MyBatis's @Param Annotaion");
                }
            }
            Object value = pm.getValue(versionField);
            TypeHandler typeHandler = versionMapping.getTypeHandler();
            JdbcType jdbcType = versionMapping.getJdbcType();

            if (value == null && jdbcType == null) {
                jdbcType = configuration.getJdbcTypeForNull();
            }
            List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
            try {
                PreparedStatement ps = (PreparedStatement) invocation.getArgs()[0];
                Object val = castTypeAndOptValue(value, parameterObject, ValueType.DECREASE);
                typeHandler.setParameter(ps, parameterMappings.size() + 1, val, jdbcType);
            } catch (TypeException | SQLException e) {
                throw new TypeException("Could not set parameters for mapping: " + parameterMappings + ". Cause: " + e, e);
            }
            return result;
        }else if ("update".equals(interceptMethod)) {
            try {
                Object result = invocation.proceed();
                if (LockerSession.getLockerFlag()) {
                    if (((Integer) result) == 0) {
                        throw new SQLException("OptimisticLock conflicts, data has been modified by other programs");
                    }

                }
                return result;
            } finally {
                LockerSession.clearLockerFlag();
            }
        }

        return invocation.proceed();
    }

    private Object castTypeAndOptValue(Object value, Object parameterObject, ValueType vt) {
        Class<?> valType = value.getClass();
        if (valType == Long.class || valType == long.class) {
            return (Long) value + vt.value;
        } else if (valType == Integer.class || valType == int.class) {
            return (Integer) value + vt.value;
        } else if (valType == Float.class || valType == float.class) {
            return (Float) value + vt.value;
        } else if (valType == Double.class || valType == double.class) {
            return (Double) value + vt.value;
        } else {
            if (parameterObject instanceof MapperMethod.ParamMap<?>) {
                throw new TypeException("All the base type parameters must add MyBatis's @Param Annotaion");
            } else {
                throw new TypeException("Property 'version' in " + parameterObject.getClass().getSimpleName() +
                        " must be [ long, int, float, double ] or [ Long, Integer, Float, Double ]");
            }
        }
    }

    private String addVersionToSql(String originalSql, String versionColumnName, Object originalVersion) {
        try {
            Statement stmt = CCJSqlParserUtil.parse(originalSql);
            if (!(stmt instanceof Update)) {
                return originalSql;
            }
            Update update = (Update) stmt;
            if (!contains(update, versionColumnName)) {
                buildVersionExpression(update, versionColumnName);
            }
            Expression where = update.getWhere();
            if (where != null) {
                AndExpression and = new AndExpression(where, buildVersionEquals(versionColumnName, originalVersion));
                update.setWhere(and);
            } else {
                update.setWhere(buildVersionEquals(versionColumnName, originalVersion));
            }
            return stmt.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return originalSql;
        }
    }

    private boolean contains(Update update, String versionColumnName) {
        List<Column> columns = update.getColumns();
        for (Column column : columns) {
            if (column.getColumnName().equalsIgnoreCase(versionColumnName)) {
                return true;
            }
        }
        return false;
    }

    private void buildVersionExpression(Update update, String versionColumnName) {

        List<Column> columns = update.getColumns();
        Column versionColumn = new Column();
        versionColumn.setColumnName(versionColumnName);
        columns.add(versionColumn);

        List<Expression> expressions = update.getExpressions();
        Addition add = new Addition();
        add.setLeftExpression(versionColumn);
        add.setRightExpression(new LongValue(1));
        expressions.add(add);
    }

    private Expression buildVersionEquals(String versionColumnName, Object originalVersion) {
        EqualsTo equal = new EqualsTo();
        Column column = new Column();
        column.setColumnName(versionColumnName);
        equal.setLeftExpression(column);
        LongValue val = new LongValue(originalVersion.toString());
        equal.setRightExpression(val);
        return equal;
    }

    private VersionLocker getVersionLocker(MappedStatement ms, BoundSql boundSql) {

        Class<?>[] paramCls = null;
        Object paramObj = boundSql.getParameterObject();

        /******************下面处理参数只能按照下面3个的顺序***********************/
        /******************Process param must order by below ***********************/
        // 1、处理@Param标记的参数
        // 1、Process @Param param
        if (paramObj instanceof MapperMethod.ParamMap<?>) {
            MapperMethod.ParamMap<?> mmp = (MapperMethod.ParamMap<?>) paramObj;
            if (null != mmp && !mmp.isEmpty()) {
                paramCls = new Class<?>[mmp.size() / 2];
                int mmpLen = mmp.size() / 2;
                for (int i = 0; i < mmpLen; i++) {
                    Object index = mmp.get("param" + (i + 1));
                    paramCls[i] = index.getClass();
                }
            }

            // 2、处理Map类型参数
            // 2、Process Map param
        } else if (paramObj instanceof Map) {
            paramCls = new Class<?>[]{Map.class};

            // 3、处理POJO实体对象类型的参数
            // 3、Process POJO entity param
        } else {
            paramCls = new Class<?>[]{paramObj.getClass()};
        }

        String id = ms.getId();
        Cache.MethodSignature vm = new MethodSignature(id, paramCls);
        VersionLocker versionLocker = versionLockerCache.getVersionLocker(vm);
        if (null != versionLocker) {
            return versionLocker;
        }

        Class<?> mapper = getMapper(ms);
        if (mapper != null) {
            Method m;
            try {
                m = mapper.getMethod(getMapperShortId(ms), paramCls);
            } catch (NoSuchMethodException | SecurityException e) {
                try {
                    m = mapper.getMethod(getMapperShortId(ms), new Class<?>[]{Object.class});
                } catch (NoSuchMethodException | SecurityException ex) {
                    throw new RuntimeException("The Map type param error." + e, e);
                }
            }
            versionLocker = m.getAnnotation(VersionLocker.class);
            if (null == versionLocker) {
                versionLocker = trueLocker;
            }
            if (!versionLockerCache.containMethodSignature(vm)) {
                versionLockerCache.cacheMethod(vm, versionLocker);
            }
            return versionLocker;
        } else {
            throw new RuntimeException("Config info error, maybe you have not config the Mapper interface");
        }
    }

    private Class<?> getMapper(MappedStatement ms) {
        String namespace = getMapperNamespace(ms);
        Collection<Class<?>> mappers = ms.getConfiguration().getMapperRegistry().getMappers();
        for (Class<?> clazz : mappers) {
            if (clazz.getName().equals(namespace)) {
                return clazz;
            }
        }
        return null;
    }

    private String getMapperNamespace(MappedStatement ms) {
        String id = ms.getId();
        int pos = id.lastIndexOf(".");
        return id.substring(0, pos);
    }

    private String getMapperShortId(MappedStatement ms) {
        String id = ms.getId();
        int pos = id.lastIndexOf(".");
        return id.substring(pos + 1);
    }

    @Override
    public Object plugin(Object target) {
        if (target instanceof StatementHandler || target instanceof ParameterHandler)
            return Plugin.wrap(target, this);
        return target;
    }

    @Override
    public void setProperties(Properties properties) {
        if (null != properties && !properties.isEmpty()) props = properties;
    }

    private enum ValueType {
        INCREASE(1), DECREASE(-1);

        private Integer value;

        private ValueType(Integer value) {
            this.value = value;
        }
    }
}