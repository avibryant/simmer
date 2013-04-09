package com.stripe

package object simmer {
	def split(str : String, delim : String) = {
        val parts = str.split(delim)
        val head = parts.head
        if(parts.size > 1 && head.size > 0)
            Some((head, str.drop(head.size + 1)))
        else
            None
	}
}