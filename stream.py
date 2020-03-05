# -*- coding: utf-8 -*-

import random

from abc import abstractmethod
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Any, AnyStr, Callable, Dict, Iterable, List, NamedTuple, Optional, Set, Sized


class Stream(object):

    @staticmethod
    def of(iterable: Iterable[Any]) -> 'Stream':
        return StreamSource(iterable)

    @staticmethod
    def empty() -> 'Stream':
        return StreamSource(())

    @staticmethod
    def range(a: int, b: int) -> 'Stream':
        return StreamSource(range(a, b))

    @staticmethod
    def range_closed(a: int, b: int) -> 'Stream':
        return StreamSource(range(a, b + 1))

    @staticmethod
    def concat(*args: 'Any') -> 'Stream':
        return StreamChain(*args)

    @staticmethod
    def random(a: int, b: int) -> 'Stream':
        return StreamGenerator(lambda: random.randint(a, b - 1))

    @staticmethod
    def random_closed(a: int, b: int) -> 'Stream':
        return StreamGenerator(lambda: random.randint(a, b))

    @staticmethod
    def generate(generator: Callable[[], Any]) -> 'Stream':
        return StreamGenerator(generator)

    @abstractmethod
    def distinct(self) -> 'Stream':
        pass

    @abstractmethod
    def filter(self, predicate: Callable[[Any], bool]) -> 'Stream':
        pass

    @abstractmethod
    def flatten(self) -> 'Stream':
        pass

    @abstractmethod
    def limit(self, limit: int) -> 'Stream':
        pass

    @abstractmethod
    def map(self, mapper: Callable[[Any], Any]) -> 'Stream':
        pass

    @abstractmethod
    def peek(self, function: Callable[[Any], Any]) -> 'Stream':
        pass

    @abstractmethod
    def skip(self, skip: int) -> 'Stream':
        pass

    @abstractmethod
    def all_match(self, predicate: Callable[[Any], bool]) -> bool:
        pass

    @abstractmethod
    def any_match(self, predicate: Callable[[Any], bool]) -> bool:
        pass

    @abstractmethod
    def average(self) -> Optional[float]:
        pass

    @abstractmethod
    def count(self) -> int:
        pass

    @abstractmethod
    def find_first(self) -> Optional[Any]:
        pass

    @abstractmethod
    def find_last(self) -> Optional[Any]:
        pass

    @abstractmethod
    def for_each(self, function: Callable[[Any], Any]) -> None:
        pass

    @abstractmethod
    def group_by(self, classifier) -> Dict[Any, Any]:
        pass

    @abstractmethod
    def join(self, separator: AnyStr) -> Optional[AnyStr]:
        pass

    @abstractmethod
    def max(self) -> Optional[Any]:
        pass

    @abstractmethod
    def min(self) -> Optional[Any]:
        pass

    @abstractmethod
    def none_match(self, predicate: Callable[[Any], bool]) -> bool:
        pass

    @abstractmethod
    def reduce(self, identity: Optional[Any], function: Callable[[Optional[Any], Any], Any]) -> Optional[Any]:
        pass

    @abstractmethod
    def reversed(self) -> 'Stream':
        pass

    @abstractmethod
    def sorted(self, *args, **kwargs) -> 'Stream':
        pass

    @abstractmethod
    def sum(self) -> Optional[Any]:
        pass

    @abstractmethod
    def summary_statistics(self) -> NamedTuple:
        pass

    @abstractmethod
    def to_dict(self, key_mapper: Callable[[Any], Any], value_mapper: Callable[[Any], Any]) -> Dict[Any, Any]:
        pass

    @abstractmethod
    def to_list(self) -> List[Any]:
        pass

    @abstractmethod
    def to_set(self) -> Set[Any]:
        pass


def stream(iterable: Iterable[Any]) -> 'Stream':
    return Stream.of(iterable)


class ParallelStream(object):

    @staticmethod
    def of(iterable: Iterable[Any], max_workers: Optional[int] = None) -> 'ParallelStream':
        return ParallelStreamSource(iterable, max_workers=max_workers)

    @staticmethod
    def empty(max_workers: Optional[int] = None) -> 'ParallelStream':
        return ParallelStreamSource((), max_workers=max_workers)

    @staticmethod
    def range(a: int, b: int, max_workers: Optional[int] = None) -> 'ParallelStream':
        return ParallelStreamSource(range(a, b), max_workers=max_workers)

    @staticmethod
    def range_closed(a: int, b: int, max_workers: Optional[int] = None) -> 'ParallelStream':
        return ParallelStreamSource(range(a, b + 1), max_workers=max_workers)

    @staticmethod
    def concat(*args: 'Any', max_workers: Optional[int] = None) -> 'ParallelStream':
        return ParallelStreamChain(*args, max_workers=max_workers)

    @staticmethod
    def random(a: int, b: int, max_workers: Optional[int] = None) -> 'ParallelStream':
        return ParallelStreamGenerator(lambda: random.randint(a, b - 1), max_workers=max_workers)

    @staticmethod
    def random_closed(a: int, b: int, max_workers: Optional[int] = None) -> 'ParallelStream':
        return ParallelStreamGenerator(lambda: random.randint(a, b), max_workers=max_workers)

    @staticmethod
    def generate(generator: Callable[[], Any], max_workers: Optional[int] = None) -> 'ParallelStream':
        return ParallelStreamGenerator(generator, max_workers=max_workers)

    @abstractmethod
    def distinct(self) -> 'ParallelStream':
        pass

    @abstractmethod
    def filter(self, predicate: Callable[[Any], bool]) -> 'ParallelStream':
        pass

    @abstractmethod
    def flatten(self) -> 'ParallelStream':
        pass

    @abstractmethod
    def limit(self, limit: int) -> 'ParallelStream':
        pass

    @abstractmethod
    def map(self, mapper: Callable[[Any], Any]) -> 'ParallelStream':
        pass

    @abstractmethod
    def peek(self, function: Callable[[Any], Any]) -> 'ParallelStream':
        pass

    @abstractmethod
    def skip(self, skip: int) -> 'ParallelStream':
        pass

    @abstractmethod
    def all_match(self, predicate: Callable[[Any], bool]) -> bool:
        pass

    @abstractmethod
    def any_match(self, predicate: Callable[[Any], bool]) -> bool:
        pass

    @abstractmethod
    def average(self) -> Optional[float]:
        pass

    @abstractmethod
    def count(self) -> int:
        pass

    @abstractmethod
    def find_any(self) -> Optional[Any]:
        pass

    @abstractmethod
    def for_each(self, function: Callable[[Any], Any]) -> None:
        pass

    @abstractmethod
    def group_by(self, classifier) -> Dict[Any, Any]:
        pass

    @abstractmethod
    def join(self, separator: AnyStr) -> Optional[AnyStr]:
        pass

    @abstractmethod
    def max(self) -> Optional[Any]:
        pass

    @abstractmethod
    def min(self) -> Optional[Any]:
        pass

    @abstractmethod
    def none_match(self, predicate: Callable[[Any], bool]) -> bool:
        pass

    @abstractmethod
    def reduce(self, identity: Optional[Any], function: Callable[[Optional[Any], Any], Any]) -> Optional[Any]:
        pass

    @abstractmethod
    def reversed(self) -> 'ParallelStream':
        pass

    @abstractmethod
    def sorted(self, *args, **kwargs) -> 'ParallelStream':
        pass

    @abstractmethod
    def sum(self) -> Optional[Any]:
        pass

    @abstractmethod
    def summary_statistics(self) -> NamedTuple:
        pass

    @abstractmethod
    def to_dict(self, key_mapper: Callable[[Any], Any], value_mapper: Callable[[Any], Any]) -> Dict[Any, Any]:
        pass

    @abstractmethod
    def to_list(self) -> List[Any]:
        pass

    @abstractmethod
    def to_set(self) -> Set[Any]:
        pass


def parallel_stream(iterable: Iterable[Any], max_workers: Optional[int] = None) -> 'ParallelStream':
    return ParallelStream.of(iterable, max_workers=max_workers)


class StreamSection(Stream, ParallelStream):
    __source: Optional['BaseStreamSource'] = None

    @property
    def source(self) -> 'BaseStreamSource':
        return self if self.__source is None else self.__source

    @source.setter
    def source(self, value: 'BaseStreamSource') -> None:
        self.__source = value

    @source.deleter
    def source(self) -> None:
        del self.__source

    next: Optional['IntermediateNode'] = None

    def distinct(self) -> 'StreamSection':
        self.next = Distinct(self.source)
        return self.next

    def filter(self, predicate: Callable[[Any], bool]) -> 'StreamSection':
        self.next = Filter(self.source, predicate)
        return self.next

    def flatten(self) -> 'StreamSection':
        self.next = Flatten(self.source)
        return self.next

    def limit(self, limit: int) -> 'StreamSection':
        self.next = Limit(self.source, limit)
        return self.next

    def map(self, mapper: Callable[[Any], Any]) -> 'StreamSection':
        self.next = Map(self.source, mapper)
        return self.next

    def peek(self, function: Callable[[Any], Any]) -> 'StreamSection':
        self.next = Peek(self.source, function)
        return self.next

    def skip(self, skip: int) -> 'StreamSection':
        self.next = Skip(self.source, skip)
        return self.next

    def all_match(self, predicate: Callable[[Any], bool]) -> bool:
        self.next = AllMatch(self.source, predicate)
        return self.next.get()

    def any_match(self, predicate: Callable[[Any], bool]) -> bool:
        self.next = AnyMatch(self.source, predicate)
        return self.next.get()

    def average(self) -> Optional[float]:
        self.next = Average(self.source)
        return self.next.get()

    def count(self) -> int:
        self.next = Count(self.source)
        return self.next.get()

    def find_any(self) -> Optional[Any]:
        return self.find_first()

    def find_first(self) -> Optional[Any]:
        self.next = FindFirst(self.source)
        return self.next.get()

    def find_last(self) -> Optional[Any]:
        self.next = FindLast(self.source)
        return self.next.get()

    def for_each(self, function: Callable[[Any], Any]) -> None:
        self.next = ForEach(self.source, function)
        return self.next.get()

    def group_by(self, classifier) -> Dict[Any, Any]:
        self.next = GroupBy(self.source, classifier)
        return self.next.get()

    def join(self, separator: AnyStr) -> Optional[AnyStr]:
        return self.reduce(None, lambda result, element: element if result is None else result + separator + element)

    def max(self) -> Optional[Any]:
        return self.reduce(None, lambda result, element: element if result is None else max(result, element))

    def min(self) -> Optional[Any]:
        return self.reduce(None, lambda result, element: element if result is None else min(result, element))

    def none_match(self, predicate: Callable[[Any], bool]) -> bool:
        self.next = AnyMatch(self.source, predicate)
        return not self.next.get()

    def reduce(self, identity: Optional[Any], function: Callable[[Optional[Any], Any], Any]) -> Optional[Any]:
        self.next = Reduce(self.source, identity, function)
        return self.next.get()

    def reversed(self) -> 'StreamSection':
        return StreamSource(reversed(self.to_list()))

    def sorted(self, *args, **kwargs) -> 'StreamSection':
        return StreamSource(sorted(self.to_list(), *args, **kwargs))

    def sum(self) -> Optional[Any]:
        return self.reduce(None, lambda result, element: element if result is None else result + element)

    def summary_statistics(self) -> NamedTuple:
        self.next = SummaryStatistics(self.source)
        return self.next.get()

    def to_dict(self, key_mapper: Callable[[Any], Any], value_mapper: Callable[[Any], Any]) -> Dict[Any, Any]:
        result = dict()
        self.for_each(lambda element: result.update({key_mapper(element): value_mapper(element)}))
        return result

    def to_list(self) -> List[Any]:
        result = list()
        self.for_each(lambda element: result.append(element))
        return result

    def to_set(self) -> Set[Any]:
        result = set()
        self.for_each(lambda element: result.add(element))
        return result


class ParallelStreamSection(StreamSection):
    executor: ThreadPoolExecutor = None

    def __init__(self, max_workers: Optional[int] = None) -> None:
        self.executor = ThreadPoolExecutor(max_workers=max_workers)


class StreamCharacteristics(object):
    size: Optional[int]
    distinct: bool

    def __init__(self, size: Optional[int] = None, distinct: bool = False) -> None:
        self.size = size
        self.distinct = distinct

    def with_size(self, size: Optional[int]) -> 'StreamCharacteristics':
        return StreamCharacteristics(size=size, distinct=self.distinct)

    def with_distinct(self, distinct: bool) -> 'StreamCharacteristics':
        return StreamCharacteristics(size=self.size, distinct=distinct)


class BaseStreamSource(StreamSection):
    initial_characteristics: StreamCharacteristics = StreamCharacteristics()

    @abstractmethod
    def feed(self) -> None:
        pass


class StreamSource(BaseStreamSource):
    iterable: Iterable[Any] = None

    def __init__(self, iterable: Iterable[Any]) -> None:
        self.iterable = iterable

        if isinstance(iterable, Sized):
            self.initial_characteristics = self.initial_characteristics.with_size(len(iterable))

    def feed(self) -> None:
        self.next.start(self.initial_characteristics)

        for element in self.iterable:
            if not self.next.needs_more():
                return

            self.next.accept(element)


class ParallelStreamSource(BaseStreamSource, ParallelStreamSection):
    iterable: Iterable[Any] = None

    def __init__(self, iterable: Iterable[Any], max_workers: Optional[int] = None) -> None:
        super(ParallelStreamSource, self).__init__(max_workers=max_workers)

        self.iterable = iterable

        if isinstance(iterable, Sized):
            self.initial_characteristics = self.initial_characteristics.with_size(len(iterable))

    def feed(self) -> None:
        self.next.start(self.initial_characteristics)

        futures = list()

        for element in self.iterable:
            if not self.next.needs_more():
                return

            futures.append(self.executor.submit(self.next.accept, element))

        for future in futures:
            future.result()


class StreamChain(BaseStreamSource):
    chain: Iterable[Any] = None

    def __init__(self, *args: Any) -> None:
        self.chain = args

    def feed(self) -> None:
        self.next.start(self.initial_characteristics)

        for element in self.chain:
            if not self.next.needs_more():
                return

            if isinstance(element, StreamSection):
                element.next = self.next
                element.source.feed()
            else:
                self.next.accept(element)


class ParallelStreamChain(BaseStreamSource, ParallelStreamSection):
    chain: Iterable[Any] = None

    def __init__(self, *args: Any, max_workers: Optional[int] = None) -> None:
        super(ParallelStreamChain, self).__init__(max_workers=max_workers)

        self.chain = args

    def feed(self) -> None:
        self.next.start(self.initial_characteristics)

        futures = list()

        for element in self.chain:
            if not self.next.needs_more():
                return

            if isinstance(element, StreamSection):
                element.next = self.next
                futures.append(self.executor.submit(element.source.feed))
            else:
                futures.append(self.executor.submit(self.next.accept, element))

        for future in futures:
            future.result()


class StreamGenerator(BaseStreamSource):
    generator: Callable[[], Any] = None

    def __init__(self, generator: Callable[[], Any]) -> None:
        self.generator = generator

    def feed(self) -> None:
        self.next.start(self.initial_characteristics)

        while self.next.needs_more():
            self.next.accept(self.generator())


class ParallelStreamGenerator(BaseStreamSource, ParallelStreamSection):
    generator: Callable[[], Any] = None

    def __init__(self, generator: Callable[[], Any], max_workers: Optional[int] = None) -> None:
        super(ParallelStreamGenerator, self).__init__(max_workers=max_workers)

        self.generator = generator

    def feed(self) -> None:
        self.next.start(self.initial_characteristics)

        futures = list()

        while self.next.needs_more():
            futures.append(self.executor.submit(self.next.accept, self.generator()))

        for future in futures:
            future.result()


class IntermediateNode(StreamSection):
    lock: Lock = None
    characteristics: StreamCharacteristics = None

    def __init__(self, source: BaseStreamSource):
        self.source = source
        self.lock = Lock()

    @abstractmethod
    def start(self, characteristics: StreamCharacteristics) -> None:
        pass

    @abstractmethod
    def accept(self, element: Any) -> None:
        pass

    @abstractmethod
    def needs_more(self) -> bool:
        pass


class TerminalNode(IntermediateNode):

    def start(self, characteristics):
        self.characteristics = characteristics

    def get(self) -> Any:
        self.source.feed()

        return self.evaluate()

    @abstractmethod
    def evaluate(self) -> Any:
        pass


class Distinct(IntermediateNode):
    used: Set[Any] = None

    def __init__(self, source: BaseStreamSource) -> None:
        super(Distinct, self).__init__(source)
        self.used = set()

    def start(self, characteristics: StreamCharacteristics) -> None:
        self.characteristics = characteristics
        self.next.start(characteristics.with_distinct(True))

    def accept(self, element: Any) -> None:
        if self.characteristics.distinct is True:
            self.next.accept(element)
            return

        with self.lock:
            if element not in self.used:
                self.used.add(element)
                self.next.accept(element)

    def needs_more(self) -> bool:
        return self.next.needs_more()


class Filter(IntermediateNode):
    predicate: Callable[[Any], bool] = None

    def __init__(self, source: BaseStreamSource, predicate: Callable[[Any], bool]) -> None:
        super(Filter, self).__init__(source)
        self.predicate = predicate

    def start(self, characteristics: StreamCharacteristics) -> None:
        self.characteristics = characteristics
        self.next.start(characteristics.with_size(None))

    def accept(self, element: Any) -> None:
        if self.predicate(element) is True:
            self.next.accept(element)

    def needs_more(self) -> bool:
        return self.next.needs_more()


class Flatten(IntermediateNode):

    def start(self, characteristics: StreamCharacteristics) -> None:
        self.characteristics = characteristics
        self.next.start(characteristics.with_size(None).with_distinct(False))

    def accept(self, element: Any) -> None:
        if isinstance(element, StreamSection):
            element.next = self.next
            element.source.feed()
        else:
            self.next.accept(element)

    def needs_more(self) -> bool:
        return self.next.needs_more()


class Limit(IntermediateNode):
    limit: int = None
    counter: int = None

    def __init__(self, source: BaseStreamSource, limit: int):
        super(Limit, self).__init__(source)
        self.limit = limit
        self.counter = 0

    def start(self, characteristics: StreamCharacteristics) -> None:
        self.characteristics = characteristics
        self.next.start(characteristics.with_size(
                None if characteristics.size is None else min(characteristics.size, self.limit)
        ))

    def accept(self, element: Any) -> None:
        with self.lock:
            accepted = self.counter < self.limit
            self.counter += 1

        if accepted is True:
            self.next.accept(element)

    def needs_more(self) -> bool:
        return self.counter < self.limit and self.next.needs_more()


class Map(IntermediateNode):
    mapper: Callable[[Any], Any] = None

    def __init__(self, source: BaseStreamSource, mapper: Callable[[Any], Any]) -> None:
        super(Map, self).__init__(source)
        self.mapper = mapper

    def start(self, characteristics: StreamCharacteristics) -> None:
        self.characteristics = characteristics
        self.next.start(characteristics.with_distinct(False))

    def accept(self, element: Any) -> None:
        self.next.accept(self.mapper(element))

    def needs_more(self) -> bool:
        return self.next.needs_more()


class Peek(IntermediateNode):
    function: Callable[[Any], Any] = None

    def __init__(self, source: BaseStreamSource, function: Callable[[Any], Any]) -> None:
        super(Peek, self).__init__(source)
        self.function = function

    def start(self, characteristics: StreamCharacteristics) -> None:
        self.characteristics = characteristics
        self.next.start(characteristics)

    def accept(self, element: Any) -> None:
        self.function(element)
        self.next.accept(element)

    def needs_more(self) -> bool:
        return self.next.needs_more()


class Skip(IntermediateNode):
    skip: int = None
    counter: int = None

    def __init__(self, source: BaseStreamSource, skip: int) -> None:
        super(Skip, self).__init__(source)
        self.skip = skip
        self.counter = 0

    def start(self, characteristics: StreamCharacteristics) -> None:
        self.characteristics = characteristics
        self.next.start(characteristics.with_size(
            None if characteristics.size is None else max(0, characteristics.size - self.skip)
        ))

    def accept(self, element: Any) -> None:
        with self.lock:
            self.counter += 1
            accepted = self.counter > self.skip

        if accepted is True:
            self.next.accept(element)

    def needs_more(self) -> bool:
        return (self.characteristics.size is None or self.characteristics.size > self.skip) and self.next.needs_more()


class AllMatch(TerminalNode):
    result: bool = None
    predicate: Callable[[Any], bool] = None

    def __init__(self, source: BaseStreamSource, predicate: Callable[[Any], bool]) -> None:
        super(AllMatch, self).__init__(source)
        self.result = True
        self.predicate = predicate

    def evaluate(self) -> bool:
        return self.result

    def accept(self, element: Any) -> None:
        with self.lock:
            self.result &= self.predicate(element) is True

    def needs_more(self) -> bool:
        return self.result is True


class AnyMatch(TerminalNode):
    result: bool = None
    predicate: Callable[[Any], bool] = None

    def __init__(self, source: BaseStreamSource, predicate: Callable[[Any], bool]) -> None:
        super(AnyMatch, self).__init__(source)
        self.result = False
        self.predicate = predicate

    def evaluate(self) -> bool:
        return self.result

    def accept(self, element: Any) -> None:
        with self.lock:
            self.result |= self.predicate(element) is True

    def needs_more(self) -> bool:
        return self.result is False


class Average(TerminalNode):
    sum: int = None
    counter: int = None

    def __init__(self, source: BaseStreamSource) -> None:
        super(Average, self).__init__(source)
        self.counter = 0

    def evaluate(self) -> Optional[float]:
        return None if self.counter == 0 else self.sum / self.counter

    def accept(self, element: Any) -> None:
        with self.lock:
            self.sum = element if self.sum is None else self.sum + element
            self.counter += 1

    def needs_more(self) -> bool:
        return True


class Count(TerminalNode):
    counter: int = None

    def __init__(self, source: BaseStreamSource) -> None:
        super(Count, self).__init__(source)
        self.counter = 0

    def evaluate(self) -> int:
        return self.counter if self.characteristics.size is None else self.characteristics.size

    def accept(self, element: Any) -> None:
        with self.lock:
            self.counter += 1

    def needs_more(self):
        return self.characteristics.size is None


class FindFirst(TerminalNode):
    result: Optional[Any] = None
    is_empty: bool = None

    def __init__(self, source: BaseStreamSource) -> None:
        super(FindFirst, self).__init__(source)
        self.is_empty = True

    def evaluate(self) -> Optional[Any]:
        return self.result

    def accept(self, element: Any) -> None:
        self.result = element
        self.is_empty = False

    def needs_more(self) -> bool:
        return self.is_empty is True


class FindLast(TerminalNode):
    result: Optional[Any] = None

    def evaluate(self) -> Optional[Any]:
        return self.result

    def accept(self, element: Any) -> None:
        self.result = element

    def needs_more(self) -> bool:
        return True


class ForEach(TerminalNode):
    function: Callable[[Any], Any] = None

    def __init__(self, source: BaseStreamSource, function: Callable[[Any], Any]) -> None:
        super(ForEach, self).__init__(source)
        self.function = function

    def evaluate(self) -> None:
        return None

    def accept(self, element: Any) -> None:
        self.function(element)

    def needs_more(self) -> bool:
        return True


class GroupBy(TerminalNode):
    dict: Dict[Any, Any] = None
    classifier: Callable[[Any], Any] = None

    def __init__(self, source: BaseStreamSource, classifier: Callable[[Any], Any]) -> None:
        super(GroupBy, self).__init__(source)
        self.dict = dict()
        self.classifier = classifier

    def evaluate(self) -> Dict[Any, Any]:
        return self.dict

    def accept(self, element: Any) -> None:
        with self.lock:
            self.dict.setdefault(self.classifier(element), list()).append(element)

    def needs_more(self) -> bool:
        return True


class Reduce(TerminalNode):
    result: Optional[Any] = None
    function: Callable[[Optional[Any], Any], Any] = None

    def __init__(
            self, source: BaseStreamSource, identity: Optional[Any], function: Callable[[Optional[Any], Any], Any]
    ) -> None:
        super(Reduce, self).__init__(source)
        self.result = identity
        self.function = function

    def evaluate(self) -> Optional[Any]:
        return self.result

    def accept(self, element: Any) -> None:
        with self.lock:
            self.result = self.function(self.result, element)

    def needs_more(self) -> bool:
        return True


class SummaryStatistics(TerminalNode):
    sum: int = None
    counter: int = None
    min: Optional[Any] = None
    max: Optional[Any] = None

    def __init__(self, source: BaseStreamSource) -> None:
        super(SummaryStatistics, self).__init__(source)
        self.counter = 0

    def evaluate(self) -> NamedTuple:
        return namedtuple('SummaryStatistics', ['average', 'count', 'min', 'max', 'sum'])(
            None if self.counter == 0 else self.sum / self.counter,
            self.counter,
            self.min,
            self.max,
            self.sum
        )

    def accept(self, element: Any) -> None:
        with self.lock:
            self.sum = element if self.sum is None else self.sum + element
            self.counter += 1
            self.min = element if self.min is None else min(self.min, element)
            self.max = element if self.max is None else max(self.max, element)

    def needs_more(self) -> bool:
        return True
