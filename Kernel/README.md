This is a draft.

# HIGH-LEVEL FEATURES
- Memory DB
- Functional Programming
- Auto Backup to file group ring (maintain diff bitmaps for all file groups)
- Data Package
	- Package includes schema, data, procedures
	- Strong typed package non-circle dependency (package can access depended packages)
	- Multiple instances of one data package
	- Objects are allocated and stored inside one package, an object cannot be stored in a package that did not create it
	- Object can be deleted, but the handle is never reused
	- A data package can be deleted as a whole operation, which require all other packages that depend on it are deleted

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
- `object Point [: BASE-TYPE, ...] {x : int = 0, y : int}`

### DATA-COLLECTION:
```
struct AttendExams{
	s : Student;
	t : Teacher;
	e : Exam;
	score : int;
}

index AttendExams {
	Hash(s);
	Hash(e);
	Unique(e, s);
	partition(e) {
		Ordered(score);
		Unique(t);
	}
}

// AttendExams becomes a data collection
```

### OBJECT-COLLECTION:
```
object Person {
	name : string,
	id : string
}

index Person {
	Hash(name);
	Unique(id);
}

// Object collection traces all created object, you don't need to explicitly put an object into a object collection.
// The index needs to be updated if the field is changed.
// You are required to assign values to all fields when creating it, if there is no default value.
// The object name becomes a data collection with one argument

object Student : Person {
}

object School {
	name : string;
}

index School {
	Unique(name);
}

struct AttendSchool {
	student : Student,
	school : School
}

index AttendSchool {
	Unique(student);
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

struct Father{father : Person; child : Person;}
index Father{Unique(child);}

struct Mother{father : Person; child : Person;}
index Mother {
	Unique(child);
}

struct Relation{parent : Person; child : Person;}
index Relation {
	partition(child) {
		Unique(Person);
	}
}
```

### Simple Query
```
query Parents(parent : Person, child : Child)
	:- Father(parent, child);
	:- Mother(parent, child);

query GrandParents(grandParent : Person, grandChild : Person)
:- {
	query(parent)
		:- Father(parent, child);
		:- Mother(parent, child);
	Parents(grandParent, parent);
}
```

### Output only argument
```
query Square(x : int, out x2 : int)
	:- x2 <- x * x;
// <- define the execution direction, it cannot run backward from x2 to x
// out keyword is required
```

### Cached Query
```
query GrandParents(grandParent : Person, grandChild : Person) index
	Hash(grandParent)
:- {
	Parents(grandParent, parent);
	Parents(parent, grandChild);
}

// When submit a query, the index for caching is used to see if it is calculated
// If not, insert an index with the "calculating" status
// Adding an existing calculating index will cause an error (stop), which is not a failure (fail to pass a filter)
```

### order_by, order_by_desc
```
struct Exams{student : string; score : int;}
index Exams {
	Unique(student);
}

query Top10(out student : string, out score : int)
:- {
	Exams(student, score);
	order_by_desc(score)->order?
		:- order < 10;
}
```

### partition
```
struct Exams{student : string; score : int;}
index Exams {
}

query Top3ScorePerStudent(student : string, out score : int, out order : int)
:- {
	Exams(student, score);
	partition(student)
		:- order_by_desc(score)->order?
			:- order < 3;
}
```

### aggregation
```
struct Exams{student : string; score : int;}
index Exams {
}

query AverageTop3ScorePerStudent(student : string, out average : int) index
	Unique(student) // should match the code, will verify
:- {
	Exams(student, score?); // bind result and create a new name: score
	partition(student)
	:- {
		order_by_desc(score)->order?
			:- order < 3 ;
		aggregate
			:- average <- sum(score) / count(score);
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
	}
}
```

# UPDATE

# EXPRESSION (cannot run backward)

# TRANSACTION and PROCEDURE

# UPDATE (schema)

# DATABASE
