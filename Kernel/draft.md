# TYPE KEYWORDS
- bool
- `u?int(8|16|32|64)?`
- `float(32|64)?`
- `string`

# DEFINITION KEYWORDS
- `type`
- `struct`
- `object`
- `enum`
- `data`
- `index`

# QUERY KEYWORDS
- `query`
- `out`

# TYPE

### PRIMITIVE-TYPE
- `bool`
- `u?int(8|16|32|64)?`
- `float(32|64)?`
- `string`
- `object`

### MISC
- `VALUE-TYPE:`: PRIMITIVE-TYPE, COMPLEX-TYPE
- `VALUE-TYPE?`: nullable

- operators:
	- `x^` : dereference, fail if null

### TYPE-DECLARATION:
- `type NAME = TYPE;`
- `enum Seasons {Spring, Summer, Autumn, Winter}`
- `struct Point {x : int, y : int}`
- `object NAME [: BASE-TYPE] STRUCT-TYPE`

### DATA-COLLECTION:
```
data AttendExams(
	s : Student,
	t : Teacher,
	e : Exam,
	score : int
) index {
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
}
data People(default p : Person) index {
	Hash(name),
	Unique(data.id)
}
// data Xs(default x : X) means every instance X will  automatically appears in Xs
// every object should have exactly one default data collection

object Student : Person {
}
data Students(default s : Student) index {
	People(data)
}

object School {
	name : string,
}
data Schools(default s : School) index {
	Unique(name)
}

data AttendSchool (
	student : Student,
	school : School
) index {
	Require(Students(student), Schools(school))
	Partition(student) {
		Unique(school)
	}
}
```

# QUERY

```
enum Gender {
	Male,
	Female
}

object Person {
	name : string,
	gender : Gender
}

data Relation(parent : Person, child : Person) index {
	Partition(child) {
		Unique(Person)
	}
}
```

### Simple Query
```
query GrandParents(grandParent : Person, grandChild : Person) :-
	Relation(grandParent, parent),
	Relation(parent, grandSon);
```

### Output only argument
```
query Square(x : int, out x2 : int) :-
	x2 <- x * x;
// out keyword is required
```

### Cached Query
```
query GrandParents(grandParent : Person, grandChild : Person) index {
	Partition(grandParent)
}:-
	Relation(grandParent, parent),
	Relation(parent, grandSon);

// When submit a query, the index for caching is used to see if it is calculated
// If not, insert an index with the "calculating" status
// Adding an existing calculating index will cause an error (stop), which is not a failure (fail to pass a filter)
```

### Aggregation

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


