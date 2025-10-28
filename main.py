"""
Simplified VM code which works for some cases.
You need extend/rewrite code to pass all cases.
"""

import builtins
import dis
import operator
import types
import typing as tp


from types import FunctionType
from typing import Any

_BinaryOperator = tp.Callable[[tp.Any, tp.Any], tp.Any]

CO_GENERATOR = 0x20

def _build_binary_ops_mapping() -> dict[int, _BinaryOperator]:
    ops: list[tuple[str, str]] = getattr(dis, "_nb_ops")

    table: dict[str, _BinaryOperator] = {
        "+": operator.add,
        "-": operator.sub,
        "*": operator.mul,
        "/": operator.truediv,
        "//": operator.floordiv,
        "%": operator.mod,
        "<<": operator.lshift,
        ">>": operator.rshift,
        "&": operator.and_,
        "^": operator.xor,
        "|": operator.or_,
        "**": operator.pow,
        "@": operator.matmul,
        "+=": operator.iadd,
        "-=": operator.isub,
        "*=": operator.imul,
        "/=": operator.itruediv,
        "//=": operator.ifloordiv,
        "%=": operator.imod,
        "<<=": operator.ilshift,
        ">>=": operator.irshift,
        "&=": operator.iand,
        "^=": operator.ixor,
        "|=": operator.ior,
        "**=": operator.ipow,
        "@=": operator.imatmul,
    }

    mapping: dict[int, _BinaryOperator] = {}
    for index, (_name, symbol) in enumerate(ops):
        func = table.get(symbol)
        if func is None:
            raise NotImplementedError("BINARY_OP symbol {symbol!r} is unknown")
        mapping[index] = func
    return mapping

_BINARY_OPS = _build_binary_ops_mapping()

def _normolize_attr_name(raw: tp.Any) -> str:
    if isinstance(raw, str):
        name = raw.split(' ', 1)[0]
        name = name.split('+', 0)[0]
        return name
    raise raw

class Frame:
    """
    Frame header in cpython with description
        https://github.com/python/cpython/blob/3.13/Include/internal/pycore_frame.h  

    Text description of frame parameters
        https://docs.python.org/3/library/inspect.html?highlight=frame#types-and-members
    """
    def __init__(self,
                 frame_code: types.CodeType,
                 frame_builtins: dict[str, tp.Any],
                 frame_globals: dict[str, tp.Any],
                 frame_locals: dict[str, tp.Any]) -> None:
        self.code = frame_code
        self.builtins = frame_builtins
        self.globals = frame_globals
        self.locals = frame_locals
        self.data_stack: tp.Any = []
        self.return_value = None
        self.pending_kw_names: tuple[str, ...] | None = None
        self.instructions = list(dis.get_instructions(self.code))
        self.offset_to_index = {instruction.offset: index for index, instruction in enumerate(self.instructions)}
        self.ip = 0
        self.last_yield: tp.Any = None

    def kw_names_op(self, name_index: int) -> None:
        const = self.code.co_consts[name_index]
        if not isinstance(const, tuple):
            raise TypeError("KW_NAMES expects a tuple of strings")
        self.pending_kw_names = const

    def top(self) -> tp.Any:
        return self.data_stack[-1]

    def pop(self) -> tp.Any:
        return self.data_stack.pop()

    def push(self, *values: tp.Any) -> None:
        self.data_stack.extend(values)

    def popn(self, n: int) -> tp.Any:
        """
        Pop a number of values from the value stack.
        A list of n values is returned, the deepest value first.
        """
        if n > 0:
            returned = self.data_stack[-n:]
            self.data_stack[-n:] = []
            return returned
        else:
            return []

    def run(self) -> tp.Any:
        while self.ip < len(self.instructions):
            outcome = self._execute_current_instruction()
            if outcome == "RETURN":
                break
        return self.return_value

    def _execute_current_instruction(self) -> tp.Any:
        ins = self.instructions[self.ip]
        handler = getattr(self, ins.opname.lower() + "_op", None)
        if handler is None:
            raise NotImplementedError(f"opcode not supported: {ins.opname}")
        arg = ins.argval
        if ins.opname == "KW_NAMES":
            arg = ins.arg
        elif arg is None and ins.arg is not None and ins.opname != "LOAD_CONST":
            arg = ins.arg
        result = handler(arg)
        if isinstance(result, int):
            self.ip = self.offset_to_index[result]
        else:
            self.ip += 1
        return result

    def prepare_generator(self) -> None:
        while self.ip < len(self.instructions):
            outcome = self._execute_current_instruction()
            if outcome == "RETURN_GENERATOR":
                return
        raise RuntimeError("RETURN_GENERATOR not encountered")

    def run_until_yield(self) -> tuple[bool, tp.Any]:
        while self.ip < len(self.instructions):
            outcome = self._execute_current_instruction()
            if outcome == "YIELD":
                return False, self.last_yield
            if outcome == "RETURN":
                return True, self.return_value
        return True, self.return_value

    def resume_op(self, arg: int) -> tp.Any:
        return None

    def push_null_op(self, arg: int) -> tp.Any:
        self.push(None)

    def precall_op(self, arg: int) -> tp.Any:
        return None
    
    def jump_forward_op(self, target_offset: int) -> int | None:
        return target_offset
    
    def jump_backward_op(self, target_offset: int) -> int | None:
        return target_offset
    
    def pop_jump_if_true_op(self, target_offset: int) -> int | None:
        v = self.pop()
        if v:
            return target_offset
        
    def pop_jump_if_false_op(self, target_offset: int) -> int | None:
        v = self.pop()
        if not v:
            return target_offset
    
    def jump_if_true_or_pop_op(self, target_offset: int) -> int | None:
        v = self.top()
        if v:
            return target_offset
        self.pop()
    
    def jump_if_false_or_pop_op(self, target_offset: int) -> int | None:
        v = self.top()
        if not v:
            return target_offset
        self.pop()
    
    def pop_jump_backward_if_false_op(self, target_offset: int) -> int | None:
        v = self.pop()
        if not v:
            return target_offset

    def nop_op(self, arg: int) -> None:
        # NOP: ничего не делает
        return

    def call_op(self, arg: int) -> None:
        """
          https://docs.python.org/release/3.13.7/library/dis.html#opcode-CALL
        Работает и для последовательности LOAD_NAME; PUSH_NULL; CALL,
        и для LOAD_GLOBAL '... + NULL'; CALL n.
        """
        arg_values = self.popn(arg)

        kw_names = self.pending_kw_names or ()
        self.pending_kw_names = None

        if kw_names:
            kw_count = len(kw_names)
            pos_values = arg_values[:-kw_count]
            kw_values = arg_values[-kw_count:]
            kwargs = dict(zip(kw_names, kw_values))
        else:
            pos_values = arg_values
            kwargs = {}

        callable_obj = self.pop()
        if callable_obj is None:
            # стек был [..., func, NULL, <args...>] — берём func ниже
            callable_obj = self.pop()
        elif not callable(callable_obj):
            # сверху лежит объект/значение, ниже — функция: перекидываем объект в args
            func = self.pop()
            pos_values = [callable_obj, *pos_values]
            callable_obj = func

        if self.data_stack and self.data_stack[-1] is None:
            self.data_stack.pop()

        self.push(callable_obj(*pos_values, **kwargs))


    def load_name_op(self, arg: str) -> None:
        """
        Partial realization

        Operation description:
              https://docs.python.org/release/3.13.7/library/dis.html#opcode-LOAD_NAME
        """
        if arg in self.locals:
            self.push(self.locals[arg])
        elif arg in self.globals:
            self.push(self.globals[arg])
        elif arg in self.builtins:
            self.push(self.builtins[arg])
        else:
            raise NameError(f"name {arg} is not defined")

    def load_global_op(self, arg: str) -> None:
        """
        Operation description:
              https://docs.python.org/release/3.13.7/library/dis.html#opcode-LOAD_GLOBAL
        """
        if arg in self.globals:
            val = self.globals[arg]
        elif arg in self.builtins:
            val = self.builtins[arg]
        else:
            raise NameError(f"name {arg} is not defined")
        
        self.push(val)

    def load_const_op(self, arg: tp.Any) -> None:
        """
        Operation description:
              https://docs.python.org/release/3.13.7/library/dis.html#opcode-LOAD_CONST
        """
        self.push(arg)
    
    def load_fast_op(self, name_or_index: tp.Any) -> None:
        # В dis: arg     — индекс (int)
        #        argval  — имя (str)
        if isinstance(name_or_index, int):
            name = self.code.co_varnames[name_or_index]
        else:
            name = name_or_index
        if name in self.locals:
            self.push(self.locals[name])
        else:
            raise UnboundLocalError(name)
    
    def copy_op(self, index: int) -> None:
        if index <= 0 or index > len(self.data_stack):
            raise IndexError("COPY out of stack range")
        self.push(self.data_stack[-index])

    def return_value_op(self, arg: tp.Any) -> None:
        """
        Operation description:
              https://docs.python.org/release/3.13.7/library/dis.html#opcode-RETURN_VALUE
        """
        self.return_value = self.pop()
        return "RETURN"

    def return_const_op(self, arg: tp.Any) -> None:
        self.return_value = arg
        return "RETURN"

    def pop_top_op(self, arg: tp.Any) -> None:
        """
        Operation description:
              https://docs.python.org/release/3.13.7/library/dis.html#opcode-POP_TOP
        """
        self.pop()

    def make_function_op(self, _flags: int | None) -> None:
        """
        В Py 3.13 у MAKE_FUNCTION обычно нет флагов: argval=None.
        Дефолты/kwdefaults проставляются позже через SET_FUNCTION_ATTRIBUTE.
        Поэтому здесь мы просто создаём функцию из code-объекта и
        при вызове берём актуальные defaults/kwdefaults с самого объекта функции.
        """
        code = self.pop()  # <code object ...> на вершине стека

        def f(*args: tp.Any, **kwargs: tp.Any) -> tp.Any:
            # ВАЖНО: тянем актуальные дефолты с объекта f, а не из захваченных пустых значений
            defaults = getattr(f, "__defaults__", ()) or ()
            kwdefaults = getattr(f, "__kwdefaults__", {}) or {}

            mapping = self.bind_args_from_code(
                code=code,
                args=args,
                kwargs=kwargs,
                defaults=defaults,
                kwdefaults=kwdefaults,
            )
            frame = Frame(code, self.builtins, self.globals, mapping)
            if code.co_flags & CO_GENERATOR:
                return _VirtualGenerator(frame)
            return frame.run()

        # Ничего не проставляем здесь — это сделает SET_FUNCTION_ATTRIBUTE позже
        self.push(f)




    def store_name_op(self, arg: str) -> None:
        """
        Operation description:
              https://docs.python.org/release/3.13.7/library/dis.html#opcode-STORE_NAME
        """
        const = self.pop()
        self.locals[arg] = const
    
    def bind_args_from_code(self, code: types.CodeType, args: tuple[Any, ...], kwargs: dict[str, Any], defaults: tuple[Any, ...] | None, kwdefaults: tuple[Any, ...] | None) -> dict[str, Any]:
        """Bind values from `args` and `kwargs` to corresponding arguments of `func`
        :param func: function to be inspected
        :param args: positional arguments to be bound
        :param kwargs: keyword arguments to be bound
        :return: `dict[argument_name] = argument_value` if binding was successful,
                raise TypeError with one of `ERR_*` error descriptions otherwise
        """

        CO_VARARGS = 4
        CO_VARKEYWORDS = 8

        ERR_TOO_MANY_POS_ARGS = 'Too many positional arguments'
        ERR_TOO_MANY_KW_ARGS = 'Too many keyword arguments'
        ERR_MULT_VALUES_FOR_ARG = 'Multiple values for arguments'
        ERR_MISSING_POS_ARGS = 'Missing positional arguments'
        ERR_MISSING_KWONLY_ARGS = 'Missing keyword-only arguments'
        ERR_POSONLY_PASSED_AS_KW = 'Positional-only argument passed as keyword argument'

        names = code.co_varnames

        has_varargs = code.co_flags & CO_VARARGS
        has_varkwargs = code.co_flags & CO_VARKEYWORDS

        defaults = defaults or ()
        kwdefaults = kwdefaults or {}

        pos = names[:code.co_posonlyargcount]
        pos_or_kwd = names[code.co_posonlyargcount:code.co_argcount]
        kwd_start = code.co_argcount
        kwd = names[kwd_start:kwd_start + code.co_kwonlyargcount]

        varkwargs_name = None
        if has_varkwargs:
            if has_varargs:
                varkwargs_name = names[code.co_argcount + code.co_kwonlyargcount + 1]
            else:
                varkwargs_index = kwd_start + code.co_kwonlyargcount
                varkwargs_name = names[varkwargs_index]

        if code.co_argcount < len(args) and not has_varargs:
            raise TypeError(ERR_TOO_MANY_POS_ARGS)

        result = {}

        for positional_arg, arg in zip(pos, args):
            result[positional_arg] = arg

        for pos_or_kwd_arg, arg in zip(pos_or_kwd, args[code.co_posonlyargcount:]):
            if pos_or_kwd_arg in result:
                raise TypeError(ERR_MULT_VALUES_FOR_ARG)
            result[pos_or_kwd_arg] = arg

        if has_varargs:
            varargs_name = names[code.co_argcount + code.co_kwonlyargcount]
            result[varargs_name] = args[code.co_argcount:]

        allowed_names = set(pos_or_kwd) | set(kwd)
        extra_kwargs: dict[str, Any] = {}

        for key, value in kwargs.items():
            if key in pos:
                if has_varkwargs:
                    extra_kwargs[key] = value
                    continue
                else:
                    raise TypeError(ERR_POSONLY_PASSED_AS_KW)

            if key in result:
                raise TypeError(ERR_MULT_VALUES_FOR_ARG)
            elif key in allowed_names:
                result[key] = value
            elif has_varkwargs:
                extra_kwargs[key] = value
            else:
                raise TypeError(ERR_TOO_MANY_KW_ARGS)

        if has_varkwargs:
            result[varkwargs_name] = extra_kwargs

        pos_all = list(pos) + list(pos_or_kwd)

        func_defaults = defaults or ()
        tail_names = pos_all[len(pos_all) - len(func_defaults):]
        for name, val in zip(tail_names, func_defaults):
            if name not in result:
                result[name] = val

        missing = [name for name in pos_all if name not in result]
        if missing:
            raise TypeError(ERR_MISSING_POS_ARGS)

        for key in kwd:
            if key not in result and key not in kwdefaults:
                raise TypeError(ERR_MISSING_KWONLY_ARGS)
            if key not in result:
                result[key] = kwdefaults[key]
        return result

    def to_bool_op(self, _arg: int | None) -> None:
        v = self.pop()
        self.push(bool(v))

    def compare_op_op(self, arg: tp.Any) -> None:
        right = self.pop()
        left = self.pop()

        # dis в 3.13 может дать '==', а может строку вроде 'bool(==)'
        # Поддержим оба случая простым разбором строки.
        op_str = None
        if isinstance(arg, str):
            # вытаскиваем оператор из 'bool(==)' -> '=='
            if '==' in arg: op_str = '=='
            elif '!=' in arg: op_str = '!='
            elif '<=' in arg: op_str = '<='
            elif '>=' in arg: op_str = '>='
            elif '<'  in arg: op_str = '<'
            elif '>'  in arg: op_str = '>'
            elif 'is not' in arg: op_str = 'is not'
            elif 'is' in arg: op_str = 'is'
            elif 'in' in arg: op_str = 'in'
            elif 'not in' in arg: op_str = 'not in'

        if op_str == '==':
            res = (left == right)
        elif op_str == '!=':
            res = (left != right)
        elif op_str == '<':
            res = (left < right)
        elif op_str == '<=':
            res = (left <= right)
        elif op_str == '>':
            res = (left > right)
        elif op_str == '>=':
            res = (left >= right)
        elif op_str == 'is':
            res = (left is right)
        elif op_str == 'is not':
            res = (left is not right)
        elif op_str == 'in':
            res = (left in right)
        elif op_str == 'not in':
            res = (left not in right)
        else:
            raise NotImplementedError(f"COMPARE_OP not supported for arg: {arg!r}")

        self.push(res)
    
    def binary_op_op(self, op_code: int) -> None:
        right = self.pop()
        left = self.pop()
        operator_func = _BINARY_OPS[op_code]
        self.push(operator_func(left, right))

    def call_intrinsic_1_op(self, intrinsic: int) -> None:
        func = self.pop()
        arg = self.pop()

        if hasattr(func, "__name__"):
            name = func.__name__

            if name == "print":
                print(arg, end='')
                self.push(None)

            elif name == "len":
                self.push(len(arg))

            elif name == "abs":
                self.push(abs(arg))

            elif name == "repr":
                self.push(repr(arg))

            elif name == "str":
                self.push(str(arg))

            elif name == "int":
                self.push(int(arg))

            else:
                raise NotImplementedError(f"CALL_INTRINSIC_1: not supported {name}")

        else:
            raise NotImplementedError(f"CALL_INTRINSIC_1: unknown intrinsic {intrinsic}")

    def store_fast_op(self, name_or_index: tp.Any) -> None:
        if isinstance(name_or_index, int):
            name = self.code.co_varnames[name_or_index]
        else:
            name = name_or_index
        value = self.pop()
        self.locals[name] = value
    
    def build_list_op(self, n: int) -> None:
        elems = self.popn(n)
        self.push(list(elems))
    
    def build_tuple_op(self, n: int) -> None:
        elems = self.popn(n)
        self.push(tuple(elems))

    def build_string_op(self, count: int) -> None:
        parts = self.popn(count)
        result = ''.join(str(part) for part in parts)
        self.push(result)

    def set_function_attribute_op(self, kind: int) -> None:
        func = self.pop()
        value = self.pop()
        if kind == 1:  # defaults
            if not isinstance(value, tuple):
                value = tuple(value)
            func.__defaults__ = value
        elif kind == 2:  # kwdefaults
            if not isinstance(value, dict):
                raise TypeError("kwdefaults must be a dict")
            func.__kwdefaults__ = value
        else:
            raise NotImplementedError(f"SET_FUNCTION_ATTRIBUTE {kind=} not implemented")
        # вернуть func на стек, как делает CPython
        self.push(func)

    
    def load_attr_op(self, name_or_flag: tp.Any) -> None:
        name = _normolize_attr_name(name_or_flag)
        obj = self.pop()
        attr = getattr(obj, name)
        self.push(attr)

    def load_method_op(self, name_or_flag: tp.Any) -> None:
        name = _normolize_attr_name(name_or_flag)
        obj = self.pop()
        attr = getattr(obj, name)
        self.push(attr)
    
    def store_global_op(self, name: str) -> None:
        value = self.pop()
        self.globals[name] = value
    
    def delete_global_op(self, name: str) -> None:
        if name in self.globals:
            del self.globals[name]
        else:
            raise NameError(f"{name=} is not defined")

    def get_iter_op(self, arg: int) -> None:
        iterable = self.pop()
        self.push(iter(iterable))

    def for_iter_op(self, target_offset: int) -> int | None:
        it = self.top()
        try:
            self.push(next(it))     # получили следующий элемент — продолжаем
            return None             # ip += 1
        except StopIteration:
            self.pop()
            return target_offset    # прыжок к END_FOR

    def end_for_op(self, arg: int) -> None:
        self.pop()  # убрать итератор со стека
        self.push(None)
    
    def unpack_sequence_op(self, count: int) -> None:
        seq = self.pop()

        it = iter(seq)

        items = []
        try:
            for _ in range(count):
                items.append(next(it))
        except StopIteration:
            raise ValueError("not enough values to unpack (expected %d)" % count)

        try:
            next(it)
            raise ValueError("too many values to unpack (expected %d)" % count)
        except StopIteration:
            pass

        for i in range(count - 1, -1, -1):
            self.push(items[i])

    
    def break_loop_op(self, arg: int) -> int | None:
        raise NotImplementedError("BREAK_LOOP not supported in this version")


    def unary_positive_op(self, _arg: tp.Any) -> None:
        self.push(+self.pop())
    
    def unary_negative_op(self, _arg: tp.Any) -> None:
        self.push(-self.pop())
    
    def unary_not_op(self, _arg: tp.Any) -> None:
        self.push(not(self.pop()))
    
    def unary_invert_op(self, _arg: tp.Any) -> None:
        self.push(~self.pop())
    
    def binary_subscr_op(self, _arg: tp.Any) -> None:
        key = self.pop()
        container = self.pop()
        self.push(container[key])
    
    def store_subscr_op(self, _arg: tp.Any) -> None:
        key = self.pop()
        container = self.pop()
        value = self.pop()       
        container[key] = value
    
    def delete_subscr_op(self, _arg: tp.Any) -> None:
        key = self.pop()
        container = self.pop()
        del container[key]

    def return_generator_op(self, _arg: tp.Any) -> str:
        return "RETURN_GENERATOR"

    def yield_value_op(self, _arg: tp.Any) -> str:
        self.last_yield = self.pop()
        return "YIELD"

    def binary_slice_op(self, _arg: tp.Any) -> None:
        end = self.pop()
        start = self.pop()
        container = self.pop()
        self.push(container[start:end])
    
    def build_slice_op(self, arg: int) -> None:
        if arg == 1:
            stop = self.pop()
            self.push(slice(stop=stop))
        elif arg == 2:
            stop = self.pop()
            start = self.pop()
            self.push(slice(start, stop))
        elif arg == 3:
            step = self.pop()
            stop = self.pop()
            start = self.pop()
            self.push(slice(start, stop, step))
        else:
            raise ValueError("Unexpected slice arg: {arg}")
    
    def store_slice_op(self, arg: int) -> None:
        end = self.pop()
        start = self.pop()
        container = self.pop()
        values = self.pop()
        container[start:end] = values
    
    def build_list_op(self, count: int) -> None:
        self.push(list(self.popn(count)))
    
    def list_append_op(self, count: int) -> None:
        value = self.pop()
        target = self.data_stack[-count]
        target.append(value)

    def list_extend_op(self, count: int) -> None:
        iterable = self.pop()
        target = self.data_stack[-count]
        target.extend(iterable)
    
    def build_map_op(self, count: int) -> None:
        items = self.popn(2 * count)
        it = iter(items)
        result: dict[tp.Any, tp.Any] = {}
        for key, value in zip(it, it):
            result[key] = value
        self.push(result)
    
    def build_const_key_map_op(self, count: int) -> None:
        keys = self.pop()
        values = self.popn(count)
        self.push(dict(zip(keys, values)))
    
    def dict_update_op(self, _arg: tp.Any) -> None:
        mapping = self.pop()
        target = self.top()
        target.update(mapping)

    def dict_merge_op(self, _arg: tp.Any) -> None:
        mapping = self.pop()
        target = self.top()
        target.update(mapping)
    
    def map_add_op(self, count: int) -> None:
        value = self.pop()
        key = self.pop()
        mapping = self.data_stack[-count]
        mapping[key] = value
    
    def build_set_op(self, count: int) -> None:
        self.push(set(self.popn(count)))
    
    def set_add_op(self, count: int) -> None:
        value = self.pop()
        target = self.data_stack[-count]
        target.add(value)
    
    def set_update_op(self, _arg: tp.Any) -> None:
        mapping = self.pop()
        target = self.top()
        if not isinstance(target, set):
            raise TypeError("SET_UPDATE requires set as target")
        target.update(mapping)

    def format_value_op(self, flags: tp.Any) -> None:
        converter: tp.Callable[[tp.Any], str] | None
        has_format: bool

        if isinstance(flags, tuple):
            converter, has_format = flags
        else:
            has_format = bool(flags & 0x04)
            index = flags & 0x03
            converter = dis.FORMAT_VALUE_CONVERTERS[index][0]

        fmt_spec = self.pop() if has_format else None
        value = self.pop()

        if converter is str:
            value = str(value)
        elif converter is repr:
            value = repr(value)
        elif converter is ascii:
            value = ascii(value)

        if fmt_spec is not None:
            formatted = format(value, fmt_spec)
        elif isinstance(value, str):
            formatted = value
        else:
            formatted = str(value)

        self.push(formatted)

    def is_op_op(self, arg: int) -> None:
        right = self.pop()
        left = self.pop()

        if arg == 0:
            result = (left is right)
        elif arg == 1:
            result = (left is not right)
        else:
            raise ValueError(f"IS_OP: invalid argument {arg}")

        self.push(result)


    # --- Frame.resume_op / precall_op ---
    def resume_op(self, arg: int) -> None:
        return  # no-op

    def precall_op(self, arg: int) -> None:
        return  # no-op


class _VirtualGenerator:
    def __init__(self, frame: Frame) -> None:
        self._frame = frame
        self._started = False
        self._finished = False

    def __iter__(self) -> "_VirtualGenerator":
        return self

    def __next__(self) -> tp.Any:
        return self.send(None)

    def send(self, value: tp.Any) -> tp.Any:
        if self._finished:
            raise StopIteration
        if not self._started:
            if value is not None:
                raise TypeError("can't send non-None value to a just-started generator")
            self._frame.prepare_generator()
            self._started = True
        self._frame.push(value)
        completed, result = self._frame.run_until_yield()
        if completed:
            self._finished = True
            raise StopIteration(result)
        return result


# --- VirtualMachine.run ---
class VirtualMachine:
    def run(self, code_obj: types.CodeType) -> None:
        globals_context: dict[str, tp.Any] = {}
        globals_context["__builtins__"] = builtins.__dict__
        frame = Frame(code_obj, builtins.__dict__, globals_context, globals_context)
        return frame.run()
