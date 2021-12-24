# -*- coding: utf-8 -*-

from py4j.java_gateway import java_import, JavaGateway

# 函数调用
gateway = JavaGateway()
result = gateway.entry_point.addition(1,2)
print(result)  # 输出：3

# 如果需反射获取类字段值，需设置auto_field=True
gateway = JavaGateway(auto_field=True)
name = gateway.entry_point.name
print(name)  # 输出：张三
