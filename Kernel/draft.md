# TYPE KEYWORDS
- bool
- `u?int(8|16|32|64)?`
- `float(32|64)?`
- `string`
- `object`

# DEFINITION KEYWORDS
- `type`
- `newtype`
- `data`
- `index`

# TYPE

### PRIMITIVE-TYPE
- `bool`
- `u?int(8|16|32|64)?`
- `float(32|64)?`
- `string`
- `object`

### COMPLEX-TYPE
- `{Spring | Summer | Autumn | Winter}`
- `{x : int, y : int, z : int}`

### MISC
- `VALUE-TYPE:`: PRIMITIVE-TYPE, COMPLEX-TYPE
- `VALUE-TYPE?`: nullable

- operators:
	- `x^` : dereference, fail if null

### TYPE-DECLARATION:
- `type NAME = TYPE;`
- `object NAME [: BASE-TYPE] STRUCT-TYPE;`

### DATA-COLLECTION:
```
data AttendExams = ({
	s : Student,
	t : Teacher,
	e : Exam,
	score : int
}) index {
	Hash(s),
	Hash(e),
	Unique(e, s)
	Partition(e) {
		Ordered(score),
		Unique(t)
	}
}
```

### OBJECT-COLLECTION:
```
object Person {
	name : string,
	id : string
};
data People = (default Person) index {
	Hash(name),
	Unique(data.id)
};
// data Xs = (default X) means every instance X will  automatically appears in Xs
// every object should have exactly one default data collection

object Student : Person {
};
data Students = (default Student) index {
	People(data)
};

object School {
	name : string,
};
data Schools = (default School) index {
	Unique(name)
};

data AttendSchool = ({
	student : Student,
	school : School
}) index {
	Require(Students(student), Schools(school))
	Partition(student) {
		Unique(school)
	}
}
```

################################################
# QUERY, CACHED QUERY AND AGGREGATION

enum Gender = Male | Female;

entity Person
(
	name		: string,
	gender		: Gender,
	parents		: data(Person),
	children	: data(Person)
) index	Ordered(name)
		;

entity index n..n Person.parents * Person.children;

# query
# query cannot call functions which contains modification expressions
query GrandParents(person : Person, grandParent : Person) :-
	person.parents(out parent),
	parent.parents(out grandParent);

# cached query
query GP_Cached(person : Person, grandParent : Person) :-
	GrandParents(person, out grandParent)
 index	Cached(person)	# Cached likes primary key index, one query can has only one Cached index
		;				# Cached(person) creates Hash(person) if not other index on (person) is used

# ordering
query ExamTop3(s : Student, e : Exam, score : int) :-
	select (s, e, score)
	from AttendExam(s, e, score)
	order_by_desc score
	fetch 0..2;

# group by
query ExamAttendCount(e: Exam, out count : int) :-
	select (e)
	from AttendExam(_, e, _)
	group_by e (count = Count(e));

# partition
query AllExamTop3(s : Student, e : Exam, score : int) :-
	select (s, e, score)
	from AttendExam(s, e, score)
	partition_by e
	(
		order_by_desc score
		fetch 0..2
	);

# inner join
query ClassExams(c : Class, e : Exam) :-
	AttendExam(out s, e, _),
	AttendClass(out c, s);

# union
# intersect

################################################
# FUNCTION

func add(in a : int, in b : int, out c : int) :-
	c = a + b;

func sqrt(in a : float, out b : float) :-
	sqrt_internal(a, out c),
	expand b = {c, -c};

# all arguments in a function should be "in" or "out"
# function will not be inlined
# function can contains collection operating statements (see exceptions)

if <expression> then <expression> else <expression>
try <expression>
exists <expression>
not <expression>
<expression> (and | or) <expression>
for <query> <expression>

################################################
# CONDITION AND EXCEPTION

# the following expressions always returns true
# if the operation failed, it throws an exception
# use "try <expression>" to turn exception to false
set Data(value1, value2, ...)
update Data(value1, value2, ...) :-
	(
		value1 < 10,
		value2 = 20
	)
remove Data(value1, value2, ...) :- condition
remove Data # remove all

# exception is not cachable, it directly turns the transaction to failure	

################################################
# VIEW AND VIEW OPERATION

# Scenarios
# 	epsilon edge removing in NFA construction
# 	state merging in DFA construction
# 	state/edge mapping
# 	build scope tree from syntax tree
# 	strong connected components in optimizing type inferencing
# 	state connectivity
# 	edge with properties

# only struct, view, default viewimpl and function has type parameters
# only functions can appear inside a view
view[T] Comparable
{
	func Compare(in a : T, in b : T, out result : int);
}

viewimpl [IntComparable : ]Comparable[int]
{
	func Compare(in a, in b, out result) :-
		result = a - b;
}

# type arguments should be specified in order, or specify nothing and do type inference
# view arguments can be specified in any order
# missing view arguments will be replaced with default implementation
func[T : cmp[int]] Sort(in x : data(T), out y : T) :- ...;
Sort(a, out b);
Sort[int](a, out b);
Sort[int : IntComparable](a, b);

################################################
# PATTERN MATCHING, MULTIPLE DISPATCHING

# when <pattern> is used in multiple dispatching, <type> should not be a query
<pattern>	::= <id>													# a name
			::= <constant>												# a constant value
			::= <id> ":" <type>											# test the class
			::= <id> [":" <type>] "{" {<field> = <pattern> ,...} "}"	# test the class and match fields
			::= "_"														# don't bind the value to a name
			::= "{" {<pattern>, ...} [ : xs] "}"						# match a list

# multiple dispatching only applies on functions
# it needs a root declaration
# functions never overload

func TheSame(a : Node, b : Node, out result : bool) :-
	result = false;

func TheSame(a : Square, b : Square, out result) :-
	result = true;

func TheSame(a : Triangle, b : Triangle, out result) :-
	result = true;

################################################
# NAMESPACE, MODULE AND DATABASE

# declare a database and make dependencies
# a database can be implemented in different files
# but one file can only implement one database
# database is not a namespace
# a file can use the namespace from the same database, or from what is imported
database <name>;
using database <name>;

# module contains several databases
# an instance of a module will creates instances for each database

using namespace a.b;

namespace a
{
	namespace b
	{
		struct Point
		(
			x : int,
			y : int
		);
	}

	namespace c
	{
		struct Vector
		(
			x : int,
			y : int
		);
	}

	data Something(a : Point, b : c.Vector);
}

################################################
# SCHEMA AND SYSTEM COLLECTION
