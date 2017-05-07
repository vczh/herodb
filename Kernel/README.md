This is a draft.

# TYPE KEYWORDS
- `bool`
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
- `order_by`
- `order_by_desc`
- `partition`
- `aggregate`

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
) index
	Hash(s),
	Hash(e),
	Unique(e, s),
	partition(e) {
		Ordered(score),
		Unique(t)
	}
;
```

### OBJECT-COLLECTION:
```
object Person {
	name : string,
	id : string
}
data People(default p : Person) index
	Hash(name),
	Unique(data.id);
// data Xs(default x : X) means every instance X will automatically appear in Xs
// every object should have exactly one default data collection

object Student : Person {
}
data Students(default s : Student) index
	People(data);

object School {
	name : string,
}
data Schools(default s : School) index
	Unique(name);

data AttendSchool (
	student : Student,
	school : School
) index
	Unique(student);
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

data Father(father : Person, child : Person) index
	Unique(child);

data Mother(father : Person, child : Person) index
	Unique(child);

data Relation(parent : Person, child : Person) index
	partition(child) {
		Unique(Person);
	};
```

### Simple Query
```
query Parents(parent : Person, child : Child) :-
	Father(parent, child)
	|
	Mother(parent, child)
	;

query GrandParents(grandParent : Person, grandChild : Person):-
	query(parent) :-
		Father(parent, child)
		|
		Mother(parent, child)
		;
	,
	Parents(grandParent, parent);
```

### Output only argument
```
query Square(x : int, out x2 : int) :-
	x2 <- x * x;
// <- define the execution direction, it cannot run backward from x2 to x
// out keyword is required
```

### Cached Query
```
query GrandParents(grandParent : Person, grandChild : Person) index
	Hash(grandParent)
	:-
	Parents(grandParent, parent),
	Parents(parent, grandSon);

// When submit a query, the index for caching is used to see if it is calculated
// If not, insert an index with the "calculating" status
// Adding an existing calculating index will cause an error (stop), which is not a failure (fail to pass a filter)
```

### order_by, order_by_desc
```
data Exams(student : string, score : int) index
	Unique(student);

query Top10(out student : string, out score : int) :-
	Exams(student, score),
	order_by_desc(score)->order? :-
		order < 10;
	;
```

### partition
```
data Exams(student : string, score : int);

query Top3ScorePerStudent(student : string, out score : int, out order : int) :-
	Exams(student, score),
	partition(student) :-
		order_by_desc(score)->order? :-
			order < 3
			;
		;
	;
```

### aggregation
```
data Exams(student : string, score : int);

query AverageTop3ScorePerStudent(student : string, out average : int) index
	Unique(student) // should match the code, will verify
	:-
	Exams(student, score?), // bind result and create a new name: score
	partition(student) :-
		order_by_desc(score)->order? :-
			order < 3
			;
		,
		aggregate :-
			average <- sum(score) / count(score);
			// if some of the arguments are aggregated
			// then the aggregated values will be duplicated
			// for example, if score is an argument, than the result looks like
			// (student, score, average)
			// (a, 10, 11)
			// (a, 12, 11)
			// (b, 10, 10)
			// but obviously it doesn't match the index Unique(student), which will lead to a compiling error
			// in this case, the correct index will be
			// partition(student) { Unique(average); } Unique(student, average);
		;
	;
```

# UPDATE

# EXPRESSION (cannot run backward)

# TRANSACTION and PROCEDURE

# UPDATE (schema)

# DATABASE
